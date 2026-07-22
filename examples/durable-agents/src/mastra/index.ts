/**
 * Durable Agents example (Mastra official), with S2 as the resumable-stream
 * backend. S2PubSub replaces the Redis-backed resumable streams from the
 * original: it delivers live and persists every chunk to S2 for replay on
 * reconnect. LibSQL keeps run metadata so a dropped client can reconnect.
 */
import { Mastra } from "@mastra/core";
import { LibSQLStore } from "@mastra/libsql";
import { S2Environment } from "@s2-dev/streamstore";

import { S2PubSub } from "../../../../src/index.js";

import { durableResearchAgent } from "./agents/research-agent.js";

if (!process.env.S2_ACCESS_TOKEN || !process.env.S2_BASIN) {
	throw new Error("Set S2_ACCESS_TOKEN, S2_BASIN and OPENAI_API_KEY.");
}

const storage = new LibSQLStore({ id: "mastra-storage", url: "file:./mastra.db" });

// Resumable streams backed by S2. `endpoints` picks up S2_ACCOUNT_ENDPOINT /
// S2_BASIN_ENDPOINT for s2-lite.
const { endpoints } = S2Environment.parse();
export const pubsub = new S2PubSub({
	accessToken: process.env.S2_ACCESS_TOKEN,
	basin: process.env.S2_BASIN,
	endpoints,
});

export const mastra = new Mastra({
	agents: { "durable-research-agent": durableResearchAgent },
	storage,
	pubsub,
});

// Re-export the durable agent: registering it on Mastra above wires it to the
// S2 pubsub, but the durable reference keeps the observe()/resumable-stream API.
export { durableResearchAgent };
