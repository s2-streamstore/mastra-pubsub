/**
 * Research agent from Mastra's official durable-agents example, wrapped as a
 * plain durable agent (resumable streams only; cache/pubsub inherited from
 * Mastra). Here the resumable-stream backend is S2 instead of Redis.
 */
import { Agent } from "@mastra/core/agent";
import { createDurableAgent } from "@mastra/core/agent/durable";
import { createTool } from "@mastra/core/tools";
import { Memory } from "@mastra/memory";
import { z } from "zod";

// Simple web search tool (simulated for demo purposes).
const webSearchTool = createTool({
	id: "web-search",
	description: "Search the web for information on a topic",
	inputSchema: z.object({
		query: z.string().describe("The search query"),
	}),
	outputSchema: z.object({
		results: z.array(
			z.object({
				title: z.string(),
				snippet: z.string(),
				url: z.string(),
			}),
		),
	}),
	execute: async (inputData: { query: string }) => {
		const { query } = inputData;
		console.log(`[web-search] Searching for: ${query}`);
		await new Promise((resolve) => setTimeout(resolve, 500));
		return {
			results: [
				{
					title: `Understanding ${query} - Comprehensive Guide`,
					snippet: `A detailed explanation of ${query} covering fundamentals and best practices.`,
					url: `https://example.com/guide/${encodeURIComponent(query)}`,
				},
				{
					title: `${query} in 2024: Latest Trends`,
					snippet: `Explore the latest developments and trends in ${query}.`,
					url: `https://example.com/trends/${encodeURIComponent(query)}`,
				},
			],
		};
	},
});

const baseAgentConfig = {
	model: process.env.MODEL ?? "openai/gpt-4o",
	instructions: `You are a research assistant that helps users find and summarize information.

When given a research topic:
1. Use the web-search tool to find relevant information
2. Analyze the search results
3. Provide a clear, well-organized summary

Be thorough but concise. Cite your sources when presenting findings.`,
	tools: {
		webSearch: webSearchTool,
	},
};

// Plain durable agent: resumable streams only, cache/pubsub inherited from Mastra.
// Memory (storage inherited from Mastra) saves conversations as threads so the
// playground can reload a thread and reattach to an in-flight run.
export const durableResearchAgent = createDurableAgent({
	agent: new Agent({
		id: "durable-research-agent",
		name: "Research Agent (Durable)",
		memory: new Memory(),
		...baseAgentConfig,
	}),
});
