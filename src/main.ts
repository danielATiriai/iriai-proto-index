import { createServer } from 'node:http'
import { connect, migrate } from './db.js'
import { connectElastic, ensureIndex } from './index/elastic.js'
import { Ingester } from './ingest/ingester.js'
import { Projector } from './ingest/projector.js'
import { createQueryServer } from './query/server.js'
import { loadSettings } from './settings.js'

// One image, two Railway services: ROLE=ingester reads Jetstream and keeps
// the index; ROLE=query serves getEvent and queryEvents. ROLE=all runs both
// in one process for local work.
const settings = loadSettings()
const log = (msg: string) => console.log(`[${settings.role}] ${msg}`)
const db = connect(settings.databaseUrl, log)
const es = connectElastic(settings.elasticUrl)

await waitFor('PostgreSQL', () => migrate(db))
await waitFor('Elasticsearch', () => ensureIndex(es, settings.elasticIndex))

let ingester: Ingester | undefined
if (settings.role !== 'query') {
  const projector = new Projector({ db, es, index: settings.elasticIndex, plcUrl: settings.plcUrl, log })
  ingester = new Ingester(settings, db, projector, log)
  await ingester.start()
}

const ingestionHealth = () => (ingester ? { ingester: { healthy: ingester.healthy, ...ingester.status } } : {})
const server =
  settings.role === 'ingester'
    ? createServer((_req, res) => {
        // 503 once the stream has been down for a minute, so a monitor sees it.
        const ok = ingester!.healthy
        res.statusCode = ok ? 200 : 503
        res.setHeader('content-type', 'application/json')
        res.end(JSON.stringify({ ok, ...ingestionHealth() }))
      })
    : createQueryServer({ db, es, index: settings.elasticIndex, ...settings, health: ingestionHealth })
server.listen(settings.port, () => log(`listening on ${settings.port}`))

for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.on(signal, async () => {
    log(`${signal}: stopping`)
    server.close()
    await ingester?.stop()
    await db.end()
    process.exit(0)
  })
}

/** Waits for a dependency at start-up for as long as it takes, rather than crash-looping. */
async function waitFor(name: string, fn: () => Promise<unknown>): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await fn()
      return
    } catch (err) {
      log(`${name} not ready (${(err as Error).message}); retrying`)
      await new Promise((r) => setTimeout(r, Math.min(10_000, 1_000 * attempt)))
    }
  }
}
