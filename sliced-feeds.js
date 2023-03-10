const { BloomFilter } = require('bloom-filters')

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
  // TODO: "sort" here is by increasing sequence number
  function getRangeOf(feedId) {
    let minSeq = Infinity
    let maxSeq = 0
    ssb.db.forEach((msg) => {
      if (msg.value.author === feedId) {
        minSeq = Math.min(minSeq, msg.value.sequence)
        maxSeq = Math.max(maxSeq, msg.value.sequence)
      }
    })
    return [minSeq, maxSeq]
  }

  function getCommonRange(feedId, remoteRange, cb) {
    const localRange = getRangeOf(feedId)
    cb(null, [
      Math.max(localRange[0], remoteRange[0]),
      Math.max(localRange[1], remoteRange[1]),
    ])
  }

  function calcBloom(feedId, range, iteration) {
    const [minSeq, maxSeq] = range
    const rangeSize = maxSeq - minSeq + 1
    const filter = new BloomFilter(10 * rangeSize, 4) // TODO tweak params
    ssb.db.forEach((msg) => {
      const { author, sequence } = msg.value
      if (author === feedId && sequence >= minSeq && sequence <= maxSeq) {
        filter.add(iteration + msg.key)
      }
    })
    return filter.saveAsJSON()
  }

  function getMessagesMissing(feedId, range, iteration, remoteBloomJSON, cb) {
    const [minSeq, maxSeq] = range
    const remoteFilter = BloomFilter.fromJSON(remoteBloomJSON)
    const missing = []
    ssb.db.forEach((msg) => {
      const { author, sequence } = msg.value
      if (author === feedId && sequence >= minSeq && sequence <= maxSeq) {
        if (!remoteFilter.has(iteration + msg.key)) {
          missing.push(msg.value)
        }
      }
    })
    cb(null, missing)
  }

  return {
    getRangeOf,
    getCommonRange,
    calcBloom,
    getMessagesMissing,
  }
}
