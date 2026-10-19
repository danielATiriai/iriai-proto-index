# Follow PlaceProto events as they happen

PlaceProto events are ordinary ATProto records in their authors' repositories, so every change to one passes through the network's public firehose. This recipe reads them from **Bluesky's public Jetstream**, filtered to the two PlaceProto record types, and resumes after a stop without losing or double-handling a change. It needs no account, key or Iriai service. The pairing step calls the Iriai query API, which is open.

Iriai does not run a stream of its own yet. A hosted change feed is planned: see [Iriai change feed (coming soon)](change-feed.md).

## What to subscribe to

A PlaceProto event is two records written together in one repository:

| Collection | What it is |
|---|---|
| `community.lexicon.calendar.event` | The event itself, readable by any calendar app |
| `app.iriai.proto.event` | The sidecar: `eventUri` (the event above, same repository) and its `location` |

Subscribe to both collections and nothing else. You do not need `app.bsky.feed.post`. Account events (`account`, `identity`, `sync`) arrive whatever the filter. An account that turns inactive or is deleted takes its events with it.

## Run it

[`scripts/stream-recipe.mjs`](../scripts/stream-recipe.mjs) is the whole recipe: Node 22 or later, no dependencies, under 100 lines.

```bash
node scripts/stream-recipe.mjs --since 24h   # first run: everything from the last 24 hours, then live
node scripts/stream-recipe.mjs               # later runs: resume from the saved cursor
```

It prints each change as it arrives, starting with its position in the stream (`seq`). For a sidecar it also asks the query API for the complete pair. Account events arrive for the whole network whatever the filter, so it only reports one for an author it has seen write a PlaceProto record:

```
#26211145361 20:41:07 create event "Taiko evening" at://did:plc:…/community.lexicon.calendar.event/3mxr…
#26211145362 20:41:07 create sidecar at://did:plc:…/app.iriai.proto.event/3mxr… for at://did:plc:…/community.lexicon.calendar.event/3mxr… at Japantown Peace Plaza
    paired: "Taiko evening"
```

The connection it opens is:

```
wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents
  ?collections=community.lexicon.calendar.event
  &collections=app.iriai.proto.event
  &cursor=<last seq you handled>
```

Each message is `{"$type": "message", "payload": {…}}`. A commit payload carries `did`, `seq`, `time`, `operation` (`create`, `update` or `delete`), `collection`, `rkey`, `rev`, `cid` and the decoded `record`. The record's address is `at://{did}/{collection}/{rkey}`. The script also reads the older v1 shape (`wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=…`, with `time_us` as the cursor), which is what the Iriai ingester uses today.

## Resume without gaps or repeats

1. **Handle, then save.** Save an event's `seq` only after you have finished handling it. The script handles events one at a time, in order, and writes the last handled `seq` (and its time) to `.placeproto-cursor` every second and on exit.
2. **Resume from the saved `seq`.** The cursor is inclusive, so the last event you handled comes again. Delivery is at least once, so other repeats are possible too. Skip anything at or before your saved `seq`, and make your writes idempotent, keyed on the record's AT-URI.
3. **Stay inside the lookback window.** Bluesky's public Jetstream replays the last **36 hours** from a cursor, with no key. The live endpoint also accepts a Unix time in microseconds as a cursor, which is how `--since` works.
4. **Past 36 hours, resync.** A cursor older than the window cannot resume. The script stops and says so rather than silently starting live. Rebuild what you need from the query API (`app.iriai.proto.queryEvents` for your area and dates), delete the cursor file and start again. Bluesky's archive replay reaches further back but needs a Jetstream API key. This recipe does not use one.

## Pair each sidecar with its event

The stream gives you records one at a time. A useful PlaceProto event needs both: the community event (name, times, description) and the sidecar (where it is). Three ways to pair them:

- **Ask the query API (simplest).** `GET https://query.iriai.app/xrpc/app.iriai.proto.getEvent?uri=<sidecar AT-URI>` returns the complete pair as one `eventView`. Use `queryEvents?eventUri=<event AT-URI>` to go from an event to its sidecar. A pair the index has not caught up with yet answers `404 SidecarNotFound` for a few seconds, so retry.
- **Keep both yourself.** Store events by AT-URI and sidecars by their `eventUri`, and treat an event as PlaceProto once both are present. The sidecar can arrive first.
- **Fetch the missing half from the author's PDS.** `com.atproto.repo.getRecord` on the author's PDS (from their DID document) returns the other record if it predates your cursor.

Edits arrive as `update`s of either record, and deletes as `delete`s. Drop the pair when either record is deleted.

## Limits

- Public Jetstream is run by Bluesky, not by Iriai. Its availability, lookback and filters are Bluesky's, and Iriai makes no promise about them. Their documentation: [Jetstream](https://bsky.network/docs/jetstream/), [Network Replay](https://bsky.network/docs/jetstream-replay/).
- Jetstream does not check repository signatures for you. It relays what the network's relay saw.
- Records are not validated. Validate a sidecar against the [iriai-proto lexicons](https://github.com/danielATiriai/iriai-proto) before you trust it. The query API only returns pairs that validate.
- The schema namespace will move from `app.iriai.proto` to `org.placeproto` once PlaceProto has governance (D-30). Read the collection names from configuration.

## How this was checked

The **Stream check** workflow (`.github/workflows/stream-check.yml`, spike 3 of the YC plan) runs [`scripts/stream-check.mjs`](../scripts/stream-check.mjs) against the v2 host above and the v1 host the ingester uses. On each host it checks four things:

- A cursor 35 hours back resumes where it asked.
- A reader that stops for 90 seconds and resumes from its saved cursor ends up with exactly the events a reader that never stopped saw: none missing, none extra.
- Reading the same stretch twice gives the same events, for a busy collection and for the PlaceProto collections over the last 30 hours.
- Positions never go backwards within a connection.

The v2 job resumes from the last handled `seq`, as this recipe does. The v1 job resumes the way the Iriai ingester does, from the last handled event's `time_us` minus 5 seconds (`src/ingest/ingester.ts`), and also checks that v1 messages still carry the fields the ingester reads.

It also runs this recipe as two runs of 300 record changes, the second resuming from the first one's cursor, and checks that together they hold exactly the 600 changes of one uninterrupted run from the same point, none handled twice. Results: see [Spike 3 results](#spike-3-results).

## Spike 3 results

Pending the first run of the Stream check workflow.
