/**
 * Durable-agent chat over S2, with one shareable link per conversation.
 *
 * Each run gets its own URL (`/chat/<runId>`). Opening that link replays the
 * whole run from S2 and then follows it live, so a refresh, a second tab, or a
 * colleague with the link all see the same transcript.
 *
 * Routes mirror Mastra's own SSE endpoints (`stream` to start, `observe` to
 * reconnect); the id lives in the path, as it does in Mastra Studio.
 */
import { readFile } from "node:fs/promises";
import {
	createServer,
	type IncomingMessage,
	type ServerResponse,
} from "node:http";

import { AGENT_STREAM_TOPIC } from "@mastra/core/agent/durable";

import {
	durableResearchAgent as agent,
	pubsub,
} from "./src/mastra/index.js";

const port = Number(process.env.PORT ?? 4111);

/**
 * Disconnect a stream that has produced nothing for this long.
 *
 * Matches the backstop on Mastra's own observe route. Without it a run that
 * never reaches a terminal event holds the response open indefinitely.
 */
const IDLE_TIMEOUT_MS = 5 * 60 * 1_000;

const publicFiles = new Map([
	["/app.js", { file: "app.js", contentType: "text/javascript; charset=utf-8" }],
	[
		"/styles.css",
		{ file: "styles.css", contentType: "text/css; charset=utf-8" },
	],
]);

/** Whether a path should serve the chat shell: `/`, `/chat/new`, `/chat/<runId>`. */
function isAppShell(pathname: string): boolean {
	return pathname === "/" || /^\/chat\/[^/]+$/.test(pathname);
}

function sendEvent(
	response: ServerResponse,
	event: string,
	data: unknown,
): void {
	if (response.destroyed || response.writableEnded) return;
	response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

function startEventStream(response: ServerResponse): void {
	response.writeHead(200, {
		"Cache-Control": "no-cache, no-transform",
		Connection: "keep-alive",
		"Content-Type": "text/event-stream; charset=utf-8",
		"X-Accel-Buffering": "no",
	});
	response.flushHeaders();
}

function textFromChunk(value: unknown): string | undefined {
	if (value === null || typeof value !== "object") return undefined;
	const chunk = value as Record<string, unknown>;
	if (typeof chunk.textDelta === "string") return chunk.textDelta;
	if (chunk.payload === null || typeof chunk.payload !== "object") {
		return undefined;
	}
	const payload = chunk.payload as Record<string, unknown>;
	return typeof payload.text === "string" ? payload.text : undefined;
}

function chunkType(value: unknown): string {
	if (value === null || typeof value !== "object") return "unknown";
	const type = (value as Record<string, unknown>).type;
	return typeof type === "string" ? type : "unknown";
}

/**
 * Whether a run still has a transcript to replay.
 *
 * `observe()` does not validate its runId: on an unknown or already-cleaned-up
 * run it yields no chunks and never finishes, which strands a client on a
 * shared link forever. S2 is the replay store, so an empty history is the
 * authoritative "nothing here" — check it before opening the observer.
 */
async function hasTranscript(runId: string): Promise<boolean> {
	const history = await pubsub.getHistory(AGENT_STREAM_TOPIC(runId));
	return history.length > 0;
}

async function pipeStream<T>(
	reader: ReadableStreamDefaultReader<T>,
	response: ServerResponse,
): Promise<void> {
	let completed = false;
	let timedOut = false;
	let idleTimer: NodeJS.Timeout | undefined;

	const cancel = (reason: string) => {
		if (!completed) void reader.cancel(reason).catch(() => {});
	};
	const onClose = () => cancel("browser disconnected");
	response.once("close", onClose);

	const resetIdleTimer = () => {
		if (idleTimer) clearTimeout(idleTimer);
		idleTimer = setTimeout(() => {
			timedOut = true;
			cancel("idle timeout");
		}, IDLE_TIMEOUT_MS);
	};
	resetIdleTimer();

	try {
		while (!response.destroyed) {
			const result = await reader.read();
			if (result.done) {
				completed = true;
				break;
			}
			resetIdleTimer();
			sendEvent(response, "chunk", {
				type: chunkType(result.value),
				text: textFromChunk(result.value),
			});
		}
		if (!response.destroyed) {
			if (timedOut) {
				sendEvent(response, "error", {
					message: "The run went idle, so the stream was closed.",
				});
			} else {
				sendEvent(response, "done", {});
			}
			response.end();
		}
	} catch (error) {
		if (!response.destroyed) {
			sendEvent(response, "error", {
				message: error instanceof Error ? error.message : String(error),
			});
			response.end();
		}
	} finally {
		if (idleTimer) clearTimeout(idleTimer);
		response.off("close", onClose);
		if (!completed) {
			await reader.cancel("stream response ended").catch(() => {});
		}
		reader.releaseLock();
	}
}

async function readPrompt(request: IncomingMessage): Promise<string> {
	let body = "";
	for await (const chunk of request) {
		body += chunk;
		if (body.length > 16_384) throw new Error("Request body is too large");
	}
	const value: unknown = JSON.parse(body);
	if (
		value === null ||
		typeof value !== "object" ||
		typeof (value as Record<string, unknown>).prompt !== "string"
	) {
		throw new Error("A string prompt is required");
	}
	const prompt = (value as { prompt: string }).prompt.trim();
	if (!prompt) throw new Error("Prompt cannot be empty");
	return prompt;
}

async function serveStatic(pathname: string, response: ServerResponse) {
	if (isAppShell(pathname)) {
		const [html, css] = await Promise.all([
			readFile(new URL("./public/index.html", import.meta.url), "utf8"),
			readFile(new URL("./public/styles.css", import.meta.url), "utf8"),
		]);
		response.writeHead(200, {
			"Cache-Control": "no-store",
			"Content-Type": "text/html; charset=utf-8",
		});
		response.end(
			html.replace(
				'<link rel="stylesheet" href="/styles.css" />',
				`<style>${css}</style>`,
			),
		);
		return true;
	}

	const asset = publicFiles.get(pathname);
	if (!asset) return false;
	response.writeHead(200, {
		"Cache-Control": "no-store",
		"Content-Type": asset.contentType,
	});
	response.end(await readFile(new URL(`./public/${asset.file}`, import.meta.url)));
	return true;
}

const server = createServer(async (request, response) => {
	try {
		const url = new URL(request.url ?? "/", `http://${request.headers.host}`);
		if (
			request.method === "GET" &&
			(await serveStatic(url.pathname, response))
		) {
			return;
		}

		if (request.method === "POST" && url.pathname === "/api/runs") {
			const prompt = await readPrompt(request);
			const started = await agent.stream(prompt);
			startEventStream(response);
			// The client swaps this into the address bar, so the link is shareable
			// from the first token onward.
			sendEvent(response, "run", { runId: started.runId, resumed: false });
			await pipeStream(started.output.fullStream.getReader(), response);
			return;
		}

		const match = /^\/api\/runs\/([^/]+)$/.exec(url.pathname);
		if (request.method === "GET" && match?.[1]) {
			const runId = decodeURIComponent(match[1]);
			if (!(await hasTranscript(runId))) {
				response.writeHead(404, { "Content-Type": "application/json" });
				response.end(
					JSON.stringify({
						error:
							"That conversation is no longer available. Its transcript has been cleaned up.",
					}),
				);
				return;
			}
			const observed = await agent.observe(runId);
			startEventStream(response);
			sendEvent(response, "run", { runId, resumed: true });
			await pipeStream(observed.output.fullStream.getReader(), response);
			return;
		}

		response.writeHead(404, { "Content-Type": "application/json" });
		response.end(JSON.stringify({ error: "Not found" }));
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		console.error("[demo] Request failed", error);
		if (!response.headersSent) {
			response.writeHead(400, { "Content-Type": "application/json" });
			response.end(JSON.stringify({ error: message }));
		} else if (!response.destroyed) {
			sendEvent(response, "error", { message });
			response.end();
		}
	}
});

server.listen(port, () => {
	console.info(`[demo] S2 durable-agent UI: http://localhost:${port}`);
});

/**
 * Re-drive runs left RUNNING by a crashed process.
 *
 * Opt-in, mirroring Mastra's `recovery.durableAgents` config, which defaults to
 * `'off'`: recovery replays the agentic loop, so it re-issues LLM calls and
 * re-runs tools. Firing it on every restart quietly spends money and repeats
 * side effects, and in a multi-replica deploy every replica races for the same
 * runs. Set `RECOVER_ON_BOOT=true` to enable it.
 */
async function recoverActiveRuns(): Promise<void> {
	if (process.env.RECOVER_ON_BOOT !== "true") return;
	const { succeeded, failed } = await agent.recoverActiveRuns();
	console.info(`[demo] Durable run recovery: ${succeeded} ok, ${failed} failed`);
}

void recoverActiveRuns().catch((error) => {
	console.error("[demo] Failed to recover active durable runs", error);
});

async function shutdown() {
	server.close();
	await pubsub.close();
}

process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
