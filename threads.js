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
  // TODO: "sort" here is by increasing (timestamp - root.timestamp)
  function getRangeOf(rootMsgId) {
    const rootMsgVal = ssb.db.get(rootMsgId)
    if (!rootMsgVal) return [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]
    let maxSeq = 0
    ssb.db.forEach((msg) => {
      if (msg.key !== rootMsgId && msg.value.content.root === rootMsgId) {
        maxSeq = Math.max(maxSeq, msg.value.timestamp - rootMsgVal.timestamp)
      }
    })
    return [0, maxSeq]
  }

  function getCommonRange(rootMsgId, remoteRange, cb) {
    const localRange = getRangeOf(rootMsgId)
    cb(null, [
      Math.min(localRange[0], remoteRange[0]),
      Math.max(localRange[1], remoteRange[1]),
    ])
  }

  function calcBloom(rootMsgId, range, iteration) {
    const [minTime, maxTime] = range
    const rangeSize = maxTime - minTime
    const filter = new BloomFilter(4 * rangeSize, 4) // TODO tweak params
    const rootMsgVal = ssb.db.get(rootMsgId)
    if (!rootMsgVal) return filter.saveAsJSON()
    ssb.db.forEach(({key, value}) => {
      const t = value.timestamp - rootMsgVal.timestamp
      if (value.content?.root === rootMsgId && t >= minTime && t <= maxTime) {
        filter.add(iteration + key)
      }
    })
    return filter.saveAsJSON()
  }

  function getMessagesMissing(rootMsgId, range, iteration, remoteBloomJSON, cb) {
    const [minTime, maxTime] = range
    const remoteFilter = BloomFilter.fromJSON(remoteBloomJSON)
    const missing = []
    const rootMsgVal = ssb.db.get(rootMsgId)
    if (!rootMsgVal) return cb(null, missing)
    ssb.db.forEach(({key, value}) => {
      const t = value.timestamp - rootMsgVal.timestamp
      if (value.content?.root === rootMsgId && t >= minTime && t <= maxTime) {
        if (!remoteFilter.has(iteration + key)) {
          missing.push(value)
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
