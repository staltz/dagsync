const { BloomFilter } = require('bloom-filters')

module.exports = function dagSyncAlgorithm(opts = {}) {
  const { direction, calcRange, estimateMsgCount, yieldMessagesIn } = opts
  if (!['forward', 'backward', 'both'].includes(direction)) {
    throw new Error('direction must be one "forward" or "backward" or "both"')
  }
  if (typeof calcRange !== 'function') {
    throw new Error('function calcRange is required')
  }
  if (typeof estimateMsgCount !== 'function') {
    throw new Error('function estimateMsgCount is required')
  }
  if (typeof yieldMessagesIn !== 'function') {
    throw new Error('function yieldMessagesIn is required')
  }

  function getCommonRange(id, remoteRange, cb) {
    const [localMin, localMax] = calcRange(id)
    const [remoteMin, remoteMax] = remoteRange
    let commonMin, commonMax
    if (direction === 'forward') {
      commonMin = Math.max(localMin, remoteMin)
      commonMax = Math.max(localMax, remoteMax)
    } else if (direction === 'backward') {
      commonMin = Math.min(localMin, remoteMin)
      commonMax = Math.min(localMax, remoteMax)
    } else if (direction === 'both') {
      commonMin = Math.min(localMin, remoteMin)
      commonMax = Math.max(localMax, remoteMax)
    }
    cb(null, [commonMin, commonMax])
  }

  function calcBloom(feedId, range, iteration) {
    const rangeSize = estimateMsgCount(range)
    const filter = new BloomFilter(10 * rangeSize, 4) // TODO tweak params
    for (const msg of yieldMessagesIn(feedId, range)) {
      filter.add(iteration + msg.key)
    }
    return filter.saveAsJSON()
  }

  function getMessagesMissing(feedId, range, iteration, remoteBloomJSON, cb) {
    const remoteFilter = BloomFilter.fromJSON(remoteBloomJSON)
    const missing = []
    for (const msg of yieldMessagesIn(feedId, range)) {
      if (!remoteFilter.has(iteration + msg.key)) {
        missing.push(msg.value)
      }
    }
    cb(null, missing)
  }

  return {
    getRangeOf: calcRange,
    getCommonRange,
    calcBloom,
    getMessagesMissing,
  }
}
