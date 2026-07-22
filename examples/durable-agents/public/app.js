const storageKey = "s2-mastra-durable-run";
const form = document.querySelector("#prompt-form");
const promptInput = document.querySelector("#prompt");
const messages = document.querySelector("#messages");
const conversation = document.querySelector("#conversation");
const emptyState = document.querySelector("#empty-state");
const userMessage = document.querySelector("#user-message");
const transcript = document.querySelector("#transcript");
const typing = document.querySelector("#typing");
const status = document.querySelector("#status");
const sendButton = document.querySelector("#send");
const newChatButton = document.querySelector("#new-chat");

let activeRequest;
let currentPrompt = "";

function readSession() {
	try {
		const value = JSON.parse(localStorage.getItem(storageKey) ?? "null");
		return typeof value?.runId === "string" && typeof value?.prompt === "string"
			? value
			: undefined;
	} catch {
		return undefined;
	}
}

function saveSession(runId) {
	localStorage.setItem(storageKey, JSON.stringify({ runId, prompt: currentPrompt }));
}

function showConversation(userText) {
	currentPrompt = userText;
	userMessage.textContent = userText;
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
		saveSession(data.runId);
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
		const response = await fetch(url, { ...options, signal: activeRequest.signal });
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
	localStorage.removeItem(storageKey);
	conversation.hidden = true;
	emptyState.hidden = false;
	currentPrompt = "";
	setBusy(false, "Ready");
	promptInput.focus();
});

const session = readSession();
if (session) {
	void connect(
		`/api/runs/${encodeURIComponent(session.runId)}`,
		{},
		session.prompt,
		true,
	);
}
