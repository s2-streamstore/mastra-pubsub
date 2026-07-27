/**
 * Drives the official durable research agent over an S2-backed resumable
 * stream:
 *
 * 1. Start a research run and read it live (every chunk persisted to S2).
 * 2. "Drop" the connection, then reconnect with the runId via observe(). The
 *    reconnect replays the whole run from S2, including anything produced
 *    while disconnected.
 *
 * Env: S2_ACCESS_TOKEN, S2_BASIN, OPENAI_API_KEY (see .env.example).
 */
import { writeSync } from "node:fs";
// Importing from index.js constructs the Mastra instance, wiring the agent to
// the S2 pubsub; we drive the durable agent reference directly.
import { durableResearchAgent as agent } from "./src/mastra/index.js";

const w = (s: string) => writeSync(1, s);

async function drain(fullStream: AsyncIterable<unknown>) {
	let count = 0;
	let text = "";
	for await (const chunk of fullStream) {
		count++;
		const c = chunk as { payload?: { text?: string }; textDelta?: string };
		const delta = c.payload?.text ?? c.textDelta;
		if (typeof delta === "string") {
			text += delta;
			w(delta);
		}
	}
	return { count, text };
}

async function main() {
	w("== 1. Live run — researching, streaming, persisting to S2 ==\n\n");
	const started = await agent.stream(
		"Research durable streams and summarize in 2 sentences.",
	);
	const live = await drain(started.output.fullStream);
	w(`\n\nrunId: ${started.runId}\n`);
	w(`live: ${live.count} chunks, ${live.text.length} chars\n\n`);
	// Without this the replay comparison below passes vacuously: two empty
	// transcripts are equal, so a failed model call would look like success.
	if (!live.text) {
		w("The live run produced no text, so there is nothing to replay.\n");
		process.exit(1);
	}

	const REFRESHES = 3;
	w(`== 2. ${REFRESHES} reconnects (client refreshes, replayed from S2) ==\n`);
	let allMatched = true;
	for (let i = 1; i <= REFRESHES; i++) {
		w(`\n-- reconnect #${i} (observe runId) --\n`);
		const observed = await agent.observe(started.runId);
		const out = await drain(observed.output.fullStream);
		const matched = out.count > 0 && out.text === live.text;
		allMatched &&= matched;
		w(`\n${matched ? "matched" : "DID NOT MATCH"} the live run\n`);
	}

	w(
		allMatched
			? "\nEvery reconnect replayed the same run from S2. Durable streams survive refreshes.\n"
			: "\nA reconnect did not match.\n",
	);
	await new Promise((res) => setTimeout(res, 150));
	process.exit(allMatched ? 0 : 1);
}

main().catch((err) => {
	console.error(err);
	process.exit(1);
});
