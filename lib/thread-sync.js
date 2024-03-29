const p = require('util').promisify
const dagSyncPlugin = require('./plugin')

module.exports = dagSyncPlugin('threadSync', (ssb, config) => ({
  haveRange(rootMsgId) {
    const rootMsgVal = ssb.db.get(rootMsgId)
    if (!rootMsgVal) return [1, 0]
    let maxTime = 0
    ssb.db.forEach((msg) => {
      if (msg.key !== rootMsgId && msg.value.content.root === rootMsgId) {
        const timestamp = Math.min(msg.timestamp, msg.value.timestamp)
        maxTime = Math.max(maxTime, timestamp - rootMsgVal.timestamp)
      }
    })
    return [0, maxTime]
  },

  wantRange(rootMsgId, localHaveRange, remoteHaveRange) {
    const [minLocalHave, maxLocalHave] = localHaveRange
    const [minRemoteHave, maxRemoteHave] = remoteHaveRange
    if (minRemoteHave !== 0) throw new Error('minRemoteHave must be 0')
    return [0, Math.max(maxLocalHave, maxRemoteHave)]
  },

  estimateMsgCount(range) {
    const [minTime, maxTime] = range
    const estimate = maxTime - minTime / 10
    if (estimate > 200) return 200
    else if (estimate < 5) return 5
    else return estimate
  },

  *yieldMsgsIn(rootMsgId, range) {
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

  async commit(msgs, cb) {
    const sortedMsgs = msgs.sort((a, b) => a.timestamp - b.timestamp)
    for (const msgVal of sortedMsgs) {
      await p(ssb.db.add)(msgVal)
    }
    cb()
  },
}))
