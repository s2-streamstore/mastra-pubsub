import { readFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import {
	durableResearchAgent as agent,
	pubsub,
} from "./src/mastra/index.js";

const port = Number(process.env.PORT ?? 4111);
const publicFiles = new Map([
	["/", { file: "index.html", contentType: "text/html; charset=utf-8" }],
	["/app.js", { file: "app.js", contentType: "text/javascript; charset=utf-8" }],
	["/styles.css", { file: "styles.css", contentType: "text/css; charset=utf-8" }],
]);

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

async function pipeStream<T>(
	reader: ReadableStreamDefaultReader<T>,
	response: ServerResponse,
): Promise<void> {
	let completed = false;
	const cancel = () => {
		if (!completed) void reader.cancel("browser disconnected").catch(() => {});
	};
	response.once("close", cancel);

	try {
		while (!response.destroyed) {
			const result = await reader.read();
			if (result.done) {
				completed = true;
				break;
			}
			sendEvent(response, "chunk", {
				type: chunkType(result.value),
				text: textFromChunk(result.value),
			});
		}
		if (!response.destroyed) {
			sendEvent(response, "done", {});
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
		response.off("close", cancel);
		if (!completed) await reader.cancel("stream response ended").catch(() => {});
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
	const asset = publicFiles.get(pathname);
	if (!asset) return false;
	let body: Buffer | string;
	if (pathname === "/") {
		const [html, css] = await Promise.all([
			readFile(new URL("./public/index.html", import.meta.url), "utf8"),
			readFile(new URL("./public/styles.css", import.meta.url), "utf8"),
		]);
		body = html.replace(
			'<link rel="stylesheet" href="/styles.css" />',
			`<style>${css}</style>`,
		);
	} else {
		body = await readFile(new URL(`./public/${asset.file}`, import.meta.url));
	}
	response.writeHead(200, {
		"Cache-Control": "no-store",
		"Content-Type": asset.contentType,
	});
	response.end(body);
	return true;
}

const server = createServer(async (request, response) => {
	try {
		const url = new URL(request.url ?? "/", `http://${request.headers.host}`);
		if (request.method === "GET" && (await serveStatic(url.pathname, response))) {
			return;
		}

		if (request.method === "POST" && url.pathname === "/api/runs") {
			const prompt = await readPrompt(request);
			const started = await agent.stream(prompt);
			startEventStream(response);
			sendEvent(response, "run", { runId: started.runId, resumed: false });
			await pipeStream(started.output.fullStream.getReader(), response);
			return;
		}

		const match = /^\/api\/runs\/([^/]+)$/.exec(url.pathname);
		if (request.method === "GET" && match?.[1]) {
			const runId = decodeURIComponent(match[1]);
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

async function recoverActiveRuns(): Promise<void> {
	const { runs } = await agent.listActiveRuns();
	await Promise.all(
		runs.map(async ({ runId }) => {
			try {
				const recovered = await agent.recover(runId);
				for await (const _chunk of recovered.output.fullStream) {
					// Drain the observer so recovery does not buffer its output in memory.
				}
				console.info(`[demo] Recovered durable run ${runId}`);
			} catch (error) {
				console.error(`[demo] Failed to recover durable run ${runId}`, error);
			}
		}),
	);
}

void recoverActiveRuns().catch((error) => {
	console.error("[demo] Failed to discover active durable runs", error);
});

async function shutdown() {
	server.close();
	await pubsub.close();
}

process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
