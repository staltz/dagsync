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
      return maxSeq - minSeq + 1
    },

    *yieldMessagesIn(feedId, range) {
      const [minSeq, maxSeq] = range
      const iter = ssb.db.filterAsIterator((msg) => {
        const { author, sequence } = msg.value
        return author === feedId && sequence >= minSeq && sequence <= maxSeq
      })
      for (const msg of iter) yield msg
    },
  })
}
