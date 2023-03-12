const p = require('util').promisify
const dagSyncPlugin = require('./plugin')

module.exports = dagSyncPlugin({
  name: 'threadSync',
  direction: 'both',

  calcRange(ssb, rootMsgId) {
    const rootMsgVal = ssb.db.get(rootMsgId)
    if (!rootMsgVal) return [Number.MAX_SAFE_INTEGER, 0]
    let maxTime = 0
    ssb.db.forEach((msg) => {
      if (msg.key !== rootMsgId && msg.value.content.root === rootMsgId) {
        const timestamp = Math.min(msg.timestamp, msg.value.timestamp)
        maxTime = Math.max(maxTime, timestamp - rootMsgVal.timestamp)
      }
    })
    return [0, maxTime]
  },

  estimateMsgCount(ssb, range) {
    const [minTime, maxTime] = range
    const estimate = maxTime - minTime / 10
    if (estimate > 200) return 200
    else if (estimate < 5) return 5
    else return estimate
  },

  *yieldMsgsIn(ssb, rootMsgId, range) {
    const [minTime, maxTime] = range
    const rootMsgVal = ssb.db.get(rootMsgId)
    if (!rootMsgVal) return
    for (const msg of ssb.db.filterAsIterator(() => true)) {
      const { key, value, timestamp } = msg
      const t = Math.min(timestamp, value.timestamp) - rootMsgVal.timestamp
      if (
        key === rootMsgId ||
        (value.content?.root === rootMsgId && t >= minTime && t <= maxTime)
      ) {
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
    const sortedMsgs = msgs.sort(
      (a, b) => a.timestamp - b.timestamp
    )
    for (const msgVal of sortedMsgs) {
      await p(ssb.db.add)(msgVal)
    }
    cb()
  },
})
