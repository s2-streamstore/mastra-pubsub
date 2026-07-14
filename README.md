# @s2-dev/mastra-pubsub

Durable [Mastra](https://mastra.ai) `PubSub` backed by [S2](https://s2.dev) for [Durable Agents](https://mastra.ai/blog/introducing-durable-agents).

Each topic maps to one S2 stream. The sequence number S2 assigns to every append becomes the event's `index`. That index is the same when the event is delivered live and when it is replayed, so Mastra's built-in replay and dedup work without any changes, so streams can work nicely across restarts and can be resumed from any offset.

## Demo

Based on Mastra's own [durable-agents example](https://github.com/mastra-ai/mastra/tree/main/examples/durable-agents) with S2 as the stream backend, adapted in [examples/durable-agents](examples/durable-agents).

![A durable agent handling a browser refresh](https://raw.githubusercontent.com/s2-streamstore/mastra-pubsub/main/assets/demo.gif)

## Install

```bash
npm install @s2-dev/mastra-pubsub @mastra/core @s2-dev/streamstore
```

`@mastra/core` is a peer dependency. `@s2-dev/streamstore` is a direct dependency.

## Setup

Create an S2 [access token](https://s2.dev/docs/access-control) and a basin with **create-stream-on-append** and **create-stream-on-read** enabled in the [dashboard](https://s2.dev/dashboard)

```ts
import { S2PubSub } from "@s2-dev/mastra-pubsub";

const pubsub = new S2PubSub({
  accessToken: process.env.S2_ACCESS_TOKEN!,
  basin: process.env.S2_BASIN!,
});
```

Pass it to Mastra wherever a `PubSub` is accepted.

## Configuration

`S2PubSubConfig`:

| Field | Description |
| --- | --- |
| `client` | An existing `S2` client. Takes precedence over `accessToken`. |
| `accessToken` | S2 access token, used to build a client when `client` is omitted. |
| `basin` | Basin for the durable streams. Enable `create-stream-on-append` and `create-stream-on-read`. |
| `endpoints` | Optional endpoint overrides, for example for `s2-lite`. |

`S2PubSubOptions`:

| Field | Description |
| --- | --- |
| `inner` | Live-delivery transport. Defaults to in-process `EventEmitterPubSub`. |
| `streamPrefix` | S2 stream-name prefix. Defaults to `mastra/durable/`. |
| `topicPrefix` | Only topics with this prefix are persisted. Defaults to `agent.stream.`. |
| `logger` | Optional logger for persistence failures. Falls back to `console`. |

## How it works

- **publish** appends the event to its topic's S2 stream, uses the assigned `seqNum` as the event `index`, then delivers it live. Topics outside `topicPrefix` are delivered live only.
- **getHistory** replays a topic from an offset by reading the stream. `index` equals `seqNum` for every event.
- **clearTopic** deletes the stream. With create-on-read enabled, a later read recreates it empty, so replay returns no history.

## Testing

The integration test needs an S2 access token:

```bash
S2_ACCESS_TOKEN=... npm test
```

To run against a local `s2-lite`, set `S2_ACCOUNT_ENDPOINT` and `S2_BASIN_ENDPOINT`.

## License

MIT
