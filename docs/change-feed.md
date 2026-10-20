# Iriai change feed (coming soon)

**Status: planned, not available.** Nothing on this page is running yet. Until it is, follow PlaceProto events with the [stream recipe](stream-recipe.md) on Bluesky's public Jetstream.

## What it will be

A change feed of PlaceProto events, served by each Iriai project's ATProto service, so an app can follow the events it cares about from the same place it signs users in, reads and writes. Unlike the raw network stream, the feed is planned to deliver **complete events**: a community calendar event and its PlaceProto sidecar together, already validated, in the same shape the query API returns.

## What is not decided

Retention, connection limits, cursor lifetime, slow-consumer handling and what happens to an expired cursor will be designed when the first feature needs the feed. No numbers are committed here, and none should be assumed from the public Jetstream's behaviour.

## Until then

- Follow changes: [stream recipe](stream-recipe.md) (public Jetstream, resume inside its 36-hour lookback).
- Look events up and search them: the query API at `https://query.iriai.app` (`app.iriai.proto.getEvent`, `app.iriai.proto.queryEvents`), open with rate limits and labelled interim.
- Contracts: [iriai-proto](https://github.com/danielATiriai/iriai-proto).

Decision record: D-13 in the MVP Implementation Inventory (the YC stream is the public ATProto stream with a documented recipe; the Iriai-hosted feed is announced as coming soon with no numbers committed).
