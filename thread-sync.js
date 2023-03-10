const dagSyncAlgorithm = require('./algorithm')

exports.name = 'threadSync'

exports.manifest = {
  getCommonRange: 'async',
  getMessagesMissing: 'async',
}

exports.permissions = {
  anonymous: {
    allow: ['getCommonRange', 'getMessagesMissing'],
  },
}

exports.init = function init(ssb, config) {
  return dagSyncAlgorithm({
    direction: 'both',

    calcRange(rootMsgId) {
      const rootMsgVal = ssb.db.get(rootMsgId)
      if (!rootMsgVal) return [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]
      let maxTime = 0
      ssb.db.forEach((msg) => {
        if (msg.key !== rootMsgId && msg.value.content.root === rootMsgId) {
          const timestamp = Math.min(msg.timestamp, msg.value.timestamp)
          maxTime = Math.max(maxTime, timestamp - rootMsgVal.timestamp)
        }
      })
      return [0, maxTime]
    },

    estimateMsgCount(range) {
      const [minTime, maxTime] = range
      const estimate = maxTime - minTime / 10
      if (estimate > 200) return 200
      else if (estimate < 5) return 5
      else return estimate
    },

    *yieldMessagesIn(rootMsgId, range) {
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
  })
}
