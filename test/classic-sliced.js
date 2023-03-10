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
  .use(require('../sliced-feeds'))

test('sync a sliced classic feed', async (t) => {
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

  await alice.db.loaded()
  await bob.db.loaded()

  const carolKeys = ssbKeys.generate('ed25519', 'carol')
  const carolMsgs = []
  const carolID = carolKeys.id
  for (let i = 1; i <= 10; i++) {
    const msg = await p(alice.db.create)({
      feedFormat: 'classic',
      content: { type: 'post', text: 'm' + i },
      keys: carolKeys,
    })
    carolMsgs.push(msg)
  }
  t.pass('alice has msgs 1..10 from carol')

  await p(bob.db.add)(carolMsgs[5].value)
  await p(bob.db.add)(carolMsgs[6].value)
  await p(bob.db.add)(carolMsgs[7].value)

  {
    const arr = bob.db
      .filterAsArray((msg) => msg.value.author === carolID)
      .map((msg) => msg.value.content.text)
    t.deepEquals(arr, ['m6', 'm7', 'm8'], 'bob has msgs 6..8 from carol')
  }

  const remoteAlice = await p(bob.connect)(alice.getAddress())
  t.pass('bob connected to alice')

  // Manual dag-sync steps
  const rangeAtBob = bob.dagsync.getRangeOf(carolID)
  const commonRange = await p(remoteAlice.dagsync.getCommonRange)(
    carolID,
    rangeAtBob
  )
  const bobGot = new Map()
  for (let i = 0; i < 4; i++) {
    const bloom = bob.dagsync.calcBloom(carolID, commonRange, i)
    const missingIter = await p(remoteAlice.dagsync.getMessagesMissing)(
      carolID,
      commonRange,
      i,
      bloom
    )
    for (const msgVal of missingIter) {
      bobGot.set(msgVal.sequence, msgVal)
    }
  }
  const missingForBob = [...bobGot.values()].sort(
    (a, b) => a.sequence - b.sequence
  )
  for (const msgVal of missingForBob) {
    await p(bob.db.add)(msgVal)
  }
  t.pass('bob got messages via dagsync')

  {
    const arr = bob.db
      .filterAsArray((msg) => msg.value.author === carolID)
      .map((msg) => msg.value.content.text)
    t.deepEquals(
      arr,
      ['m6', 'm7', 'm8', 'm9', 'm10'],
      'bob has msgs 6..10 from carol'
    )
  }

  await p(remoteAlice.close)(true)
  await p(alice.close)(true)
  await p(bob.close)(true)
})
