const dagSyncAlgorithm = require('./algorithm')

exports.name = 'dagsync'

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
          maxTime = Math.max(
            maxTime,
            msg.value.timestamp - rootMsgVal.timestamp
          )
        }
      })
      return [0, maxTime]
    },

    estimateMsgCount(range) {
      const [minTime, maxTime] = range
      return (maxTime - minTime) / 10
    },

    *yieldMessagesIn(rootMsgId, range) {
      const [minTime, maxTime] = range
      const rootMsgVal = ssb.db.get(rootMsgId)
      if (!rootMsgVal) return
      for (const msg of ssb.db.filterAsIterator(() => true)) {
        const value = msg.value
        const t = value.timestamp - rootMsgVal.timestamp
        if (value.content?.root === rootMsgId && t >= minTime && t <= maxTime) {
          yield msg
        }
      }
    },
  })
}
