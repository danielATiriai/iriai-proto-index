// The contracts this service runs against: iriai-proto v0.1.0, vendored from
// the checksummed release bundle into contracts/ (see contracts/SOURCE.json
// and scripts/check-contracts.sh). location.ts and sidecar.ts beside this file
// are byte-for-byte copies of src/location.ts and src/sidecar.ts at the
// release commit; this file stands in for the release's src/contracts.ts and
// exports the same names, read from contracts/ instead of the package root.
import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Lexicons, type LexiconDoc } from '@atproto/lexicon'

/** dist/src/contracts/contracts.js lives three levels below the repository root. */
export const contractsRoot = fileURLToPath(new URL('../../../contracts/', import.meta.url))

export interface ProtoConfig {
  namespace: string
  publisher: { handle: string; did: string; authorityRecord: string }
  communityEventCollection: string
}

/** The namespace is read from the release configuration, never hard-coded (D-30). */
export const config: ProtoConfig = JSON.parse(readFileSync(join(contractsRoot, 'iriai-proto.config.json'), 'utf8'))

export const eventSidecarNsid = `${config.namespace}.event`
export const defsNsid = `${config.namespace}.defs`

function readLexiconDir(dir: string): LexiconDoc[] {
  const docs: LexiconDoc[] = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) docs.push(...readLexiconDir(path))
    else if (entry.name.endsWith('.json')) docs.push(JSON.parse(readFileSync(path, 'utf8')))
  }
  return docs
}

export const lexicons = new Lexicons(readLexiconDir(join(contractsRoot, 'lexicons')))
