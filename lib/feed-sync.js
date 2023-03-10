const dagSyncAlgorithm = require('./algorithm')

exports.name = 'feedSync'

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
    direction: 'forward',

    calcRange(feedId) {
      let minSeq = Infinity
      let maxSeq = 0
      ssb.db.forEach((msg) => {
        if (msg.value.author === feedId) {
          minSeq = Math.min(minSeq, msg.value.sequence)
          maxSeq = Math.max(maxSeq, msg.value.sequence)
        }
      })
      return [minSeq, maxSeq]
    },

    estimateMsgCount(range) {
      const [minSeq, maxSeq] = range
      const estimate = maxSeq - minSeq + 1
      if (estimate > 1000) return 1000
      else if (estimate < 5) return 5
      else return estimate
    },

    *yieldMessagesIn(feedId, range) {
      const [minSeq, maxSeq] = range
      for (const msg of ssb.db.filterAsIterator(() => true)) {
        const { author, sequence } = msg.value
        if (author === feedId && sequence >= minSeq && sequence <= maxSeq) {
          yield msg
        }
      }
    },
  })
}
