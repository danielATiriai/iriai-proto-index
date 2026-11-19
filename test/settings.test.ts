import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { STEWARD_DID, loadSettings } from '../src/settings.js'

const base = { DATABASE_URL: 'postgres://db' }
const key = Buffer.alloc(32, 7).toString('base64')

describe('settings', () => {
  it('loads an ingester from the variables Railway sets, backfilling the steward by default', () => {
    const s = loadSettings({ ...base, ROLE: 'ingester' })
    assert.equal(s.role, 'ingester')
    assert.deepEqual(s.approvedAccounts, [STEWARD_DID])
    assert.equal(s.port, 8080)
  })

  it('loads a query service and requires a 32-byte cursor key', () => {
    assert.equal(loadSettings({ ...base, ROLE: 'query', CURSOR_KEY: key }).cursorKey.length, 32)
    assert.throws(() => loadSettings({ ...base, ROLE: 'query' }), /CURSOR_KEY/)
    assert.throws(() => loadSettings({ ...base, ROLE: 'query', CURSOR_KEY: Buffer.alloc(16).toString('base64') }), /CURSOR_KEY/)
  })

  it('reads a list of approved accounts and refuses an unknown role', () => {
    const s = loadSettings({ ...base, ROLE: 'ingester', APPROVED_ACCOUNTS: 'did:plc:a, did:plc:b,' })
    assert.deepEqual(s.approvedAccounts, ['did:plc:a', 'did:plc:b'])
    assert.throws(() => loadSettings({ ...base, ROLE: 'indexer' }), /ROLE/)
    assert.throws(() => loadSettings({ ROLE: 'ingester' }), /DATABASE_URL/)
  })

  it('takes a p95 threshold only on a histogram bucket edge', () => {
    assert.equal(loadSettings({ ...base, ROLE: 'ingester', SEARCH_ON_P95_MS: '300' }).thresholds.onP95Ms, 300)
    assert.throws(() => loadSettings({ ...base, ROLE: 'ingester', SEARCH_ON_P95_MS: '255' }), /bucket edge/)
  })
})
