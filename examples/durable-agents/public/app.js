/**
 * Each conversation lives at its own URL (`/chat/<runId>`), so a refresh, a new
 * tab, or a shared link all replay the same run from S2. The id goes in the path
 * rather than localStorage, matching how Mastra Studio addresses a chat.
 */
const form = document.querySelector("#prompt-form");
const promptInput = document.querySelector("#prompt");
const messages = document.querySelector("#messages");
const conversation = document.querySelector("#conversation");
const emptyState = document.querySelector("#empty-state");
const userTurn = document.querySelector("#user-turn");
const userMessage = document.querySelector("#user-message");
const sharedNote = document.querySelector("#shared-note");
const transcript = document.querySelector("#transcript");
const typing = document.querySelector("#typing");
const status = document.querySelector("#status");
const sendButton = document.querySelector("#send");
const newChatButton = document.querySelector("#new-chat");
const copyLinkButton = document.querySelector("#copy-link");

let activeRequest;

/** The run this page is showing, taken from `/chat/<runId>`. */
function runIdFromPath() {
	const match = /^\/chat\/([^/]+)$/.exec(window.location.pathname);
	return match ? decodeURIComponent(match[1]) : undefined;
}

/** Point the address bar at this run without reloading. */
function showRunInUrl(runId) {
	const path = `/chat/${encodeURIComponent(runId)}`;
	if (window.location.pathname !== path) {
		window.history.replaceState({}, "", path);
	}
	copyLinkButton.hidden = false;
	copyLinkButton.textContent = "Copy link";
}

/**
 * Open the conversation view.
 *
 * `userText` is undefined for a reopened link: the transcript S2 replays is the
 * agent's output, so the original message is genuinely not recoverable. Say so
 * rather than inventing one.
 */
function showConversation(userText) {
	const known = typeof userText === "string";
	userMessage.textContent = known ? userText : "";
	userTurn.hidden = !known;
	sharedNote.hidden = known;
	transcript.textContent = "";
	emptyState.hidden = true;
	conversation.hidden = false;
	typing.hidden = false;
}

function setBusy(busy, label) {
	status.textContent = label;
	sendButton.disabled = busy;
	promptInput.disabled = busy;
	typing.hidden = !busy;
}

function handleEvent(name, data) {
	if (name === "run") {
		showRunInUrl(data.runId);
		status.textContent = data.resumed ? "Resumed from S2" : "Streaming";
		return;
	}
	if (name === "chunk") {
		if (typeof data.text === "string") transcript.textContent += data.text;
		messages.scrollTop = messages.scrollHeight;
		return;
	}
	if (name === "done") {
		setBusy(false, "Complete");
		return;
	}
	if (name === "error") throw new Error(data.message ?? "Stream failed");
}

async function consumeEvents(response) {
	if (!response.ok) {
		const body = await response.json().catch(() => ({}));
		throw new Error(body.error ?? `Request failed (${response.status})`);
	}
	if (!response.body) throw new Error("Streaming response is unavailable");

	const reader = response.body.getReader();
	const decoder = new TextDecoder();
	let buffer = "";
	while (true) {
		const { done, value } = await reader.read();
		buffer += decoder.decode(value, { stream: !done }).replaceAll("\r\n", "\n");
		let boundary = buffer.indexOf("\n\n");
		while (boundary !== -1) {
			const block = buffer.slice(0, boundary);
			buffer = buffer.slice(boundary + 2);
			const event = block.match(/^event: (.+)$/m)?.[1] ?? "message";
			const data = block
				.split("\n")
				.filter((line) => line.startsWith("data: "))
				.map((line) => line.slice(6))
				.join("\n");
			if (data) handleEvent(event, JSON.parse(data));
			boundary = buffer.indexOf("\n\n");
		}
		if (done) break;
	}
}

async function connect(url, options, userText, resumed) {
	activeRequest?.abort();
	activeRequest = new AbortController();
	showConversation(userText);
	setBusy(true, resumed ? "Replaying from S2" : "Thinking");

	try {
		const response = await fetch(url, {
			...options,
			signal: activeRequest.signal,
		});
		await consumeEvents(response);
	} catch (error) {
		if (error.name === "AbortError") return;
		transcript.textContent = `Something went wrong: ${error.message}`;
		setBusy(false, "Error");
	}
}

function resizeInput() {
	promptInput.style.height = "auto";
	promptInput.style.height = `${Math.min(promptInput.scrollHeight, 150)}px`;
}

form.addEventListener("submit", (event) => {
	event.preventDefault();
	const value = promptInput.value.trim();
	if (!value) return;
	promptInput.value = "";
	resizeInput();
	void connect(
		"/api/runs",
		{
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ prompt: value }),
		},
		value,
		false,
	);
});

promptInput.addEventListener("input", resizeInput);
promptInput.addEventListener("keydown", (event) => {
	if (event.key === "Enter" && !event.shiftKey) {
		event.preventDefault();
		form.requestSubmit();
	}
});

newChatButton.addEventListener("click", () => {
	activeRequest?.abort();
	// A fresh chat is a fresh URL; the previous one keeps working.
	window.history.replaceState({}, "", "/");
	copyLinkButton.hidden = true;
	conversation.hidden = true;
	emptyState.hidden = false;
	setBusy(false, "Ready");
	promptInput.focus();
});

copyLinkButton.addEventListener("click", async () => {
	try {
		await navigator.clipboard.writeText(window.location.href);
		copyLinkButton.textContent = "Copied";
	} catch {
		copyLinkButton.textContent = "Copy failed";
	}
	setTimeout(() => {
		copyLinkButton.textContent = "Copy link";
	}, 1_500);
});

const openRunId = runIdFromPath();
if (openRunId) {
	void connect(`/api/runs/${encodeURIComponent(openRunId)}`, {}, undefined, true);
}
