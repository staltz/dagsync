const p = require('util').promisify
const dagSyncPlugin = require('./plugin')

module.exports = dagSyncPlugin({
  name: 'feedSync',
  direction: 'forward',

  calcRange(ssb, feedId) {
    let minSeq = Number.MAX_SAFE_INTEGER
    let maxSeq = 0
    ssb.db.forEach((msg) => {
      if (msg.value.author === feedId) {
        minSeq = Math.min(minSeq, msg.value.sequence)
        maxSeq = Math.max(maxSeq, msg.value.sequence)
      }
    })
    return [minSeq, maxSeq]
  },

  estimateMsgCount(ssb, range) {
    const [minSeq, maxSeq] = range
    const estimate = maxSeq - minSeq + 1
    if (estimate > 1000) return 1000
    else if (estimate < 5) return 5
    else return estimate
  },

  *yieldMsgsIn(ssb, feedId, range) {
    const [minSeq, maxSeq] = range
    for (const msg of ssb.db.filterAsIterator(() => true)) {
      const { author, sequence } = msg.value
      if (author === feedId && sequence >= minSeq && sequence <= maxSeq) {
        yield msg
      }
    }
  },

  getMsgs(ssb, msgKeys, cb) {
    const msgs = []
    for (const msgKey of msgKeys) {
      const msg = ssb.db.get(msgKey)
      if (msg) msgs.push(msg)
    }
    cb(null, msgs)
  },

  async sink(ssb, msgs, cb) {
    const sortedMsgs = msgs.sort((a, b) => a.sequence - b.sequence)
    for (const msgVal of sortedMsgs) {
      await p(ssb.db.add)(msgVal)
    }
    cb()
  },
})
