const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const os = require('os')
const rimraf = require('rimraf')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const p = require('util').promisify

const createSSB = SecretStack({ appKey: caps.shs })
  .use(require('ssb-memdb'))
  .use(require('ssb-classic'))
  .use(require('ssb-box'))
  .use(require('../threads'))

/*
BEFORE dagsync:
```mermaid
graph TB;
  subgraph Bob
    direction TB
    rootAb[root by A]
    replyB1b[reply by B]
    replyB2b[reply by B]
    replyD1b[reply by D]
    rootAb-->replyB1b-->replyB2b & replyD1b
  end
  subgraph Alice
    direction TB
    rootAa[root by A]
    replyB1a[reply by B]
    replyB2a[reply by B]
    replyC1a[reply by C]
    rootAa-->replyB1a-->replyB2a
    rootAa-->replyC1a
  end
```

AFTER dagsync:
```mermaid
graph TB;
  subgraph Bob
    rootA[root by A]
    replyB1[reply by B]
    replyB2[reply by B]
    replyC1[reply by C]
    replyD1[reply by D]
    rootA-->replyB1-->replyB2 & replyD1
    rootA-->replyC1
  end
```
*/
test('replicate a thread', async (t) => {
const ALICE_DIR = path.join(os.tmpdir(), 'dagsync-alice')
const BOB_DIR = path.join(os.tmpdir(), 'dagsync-bob')

rimraf.sync(ALICE_DIR)
rimraf.sync(BOB_DIR)

  const alice = createSSB({
    keys: ssbKeys.generate('ed25519', 'alice'),
    path: ALICE_DIR,
  })

  const bob = createSSB({
    keys: ssbKeys.generate('ed25519', 'bob'),
    path: BOB_DIR,
  })

  const carolKeys = ssbKeys.generate('ed25519', 'carol')
  const carolID = carolKeys.id

  const daveKeys = ssbKeys.generate('ed25519', 'dave')
  const daveID = daveKeys.id

  await alice.db.loaded()
  await bob.db.loaded()

  const rootA = await p(alice.db.create)({
    feedFormat: 'classic',
    content: { type: 'post', text: 'A' },
    keys: alice.config.keys,
  })
  await p(bob.db.add)(rootA.value)

  await p(setTimeout)(10)

  const replyB1 = await p(bob.db.create)({
    feedFormat: 'classic',
    content: { type: 'post', text: 'B1', root: rootA.key, branch: rootA.key },
    keys: bob.config.keys,
  })

  await p(setTimeout)(10)

  const replyB2 = await p(bob.db.create)({
    feedFormat: 'classic',
    content: { type: 'post', text: 'B2', root: rootA.key, branch: replyB1.key },
    keys: bob.config.keys,
  })
  await p(alice.db.add)(replyB1.value)
  await p(alice.db.add)(replyB2.value)

  await p(setTimeout)(10)

  const replyC1 = await p(alice.db.create)({
    feedFormat: 'classic',
    content: { type: 'post', text: 'C1', root: rootA.key, branch: rootA.key },
    keys: carolKeys,
  })

  await p(setTimeout)(10)

  const replyD1 = await p(bob.db.create)({
    feedFormat: 'classic',
    content: { type: 'post', text: 'D1', root: rootA.key, branch: replyB1.key },
    keys: daveKeys,
  })

  t.deepEquals(
    alice.db.filterAsArray((msg) => true).map((msg) => msg.value.content.text),
    ['A', 'B1', 'B2', 'C1'],
    'alice has a portion of the thread'
  )

  t.deepEquals(
    bob.db.filterAsArray((msg) => true).map((msg) => msg.value.content.text),
    ['A', 'B1', 'B2', 'D1'],
    'bob has another portion of the thread'
  )

  const remoteAlice = await p(bob.connect)(alice.getAddress())
  t.pass('bob connected to alice')

  // Manual dag-sync steps
  try {
    const rangeAtBob = bob.dagsync.getRangeOf(rootA.key)
    const commonRange = await p(remoteAlice.dagsync.getCommonRange)(
      rootA.key,
      rangeAtBob
    )
    const bobGot = new Map()
    for (let i = 0; i < 4; i++) {
      const bloom = bob.dagsync.calcBloom(rootA.key, commonRange, i)
      const missingIter = await p(remoteAlice.dagsync.getMessagesMissing)(
        rootA.key,
        commonRange,
        i,
        bloom
      )
      for (const msgVal of missingIter) {
        bobGot.set(msgVal.sequence, msgVal)
      }
    }
    const missingForBob = [...bobGot.values()].sort(
      (a, b) => a.timestamp - b.timestamp
    )
    for (const msgVal of missingForBob) {
      await p(bob.db.add)(msgVal)
    }
  } catch (err) {
    console.error(err)
  }
  t.pass('bob got messages via dagsync')

  t.deepEquals(
    bob.db.filterAsArray((msg) => true).map((msg) => msg.value.content.text),
    ['A', 'B1', 'B2', 'D1', 'C1'],
    'bob has the full thread'
  )

  await p(remoteAlice.close)(true)
  await p(alice.close)(true)
  await p(bob.close)(true)
})

test.skip('sync a thread where one peer does not have the root', async (t) => {
  // FIXME:
})
