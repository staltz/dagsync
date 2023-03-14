const { BloomFilter } = require('bloom-filters')

module.exports = function dagSyncAlgorithm(opts = {}) {
  const {
    direction,
    calcRange,
    estimateMsgCount,
    yieldMsgsIn,
    getMsgs,
    sink,
  } = opts
  if (!['forward', 'backward', 'both'].includes(direction)) {
    throw new Error('direction must be one "forward" or "backward" or "both"')
  }
  if (typeof calcRange !== 'function') {
    throw new Error('function calcRange is required')
  }
  if (typeof estimateMsgCount !== 'function') {
    throw new Error('function estimateMsgCount is required')
  }
  if (typeof yieldMsgsIn !== 'function') {
    throw new Error('function yieldMsgsIn is required')
  }
  if (typeof getMsgs !== 'function') {
    throw new Error('function getMsgs is required')
  }
  if (typeof sink !== 'function') {
    throw new Error('function sink is required')
  }

  function commonRangeFor(id, remoteRange) {
    const localRange = calcRange(ssb, id)
    const [localMin, localMax] = localRange
    const [remoteMin, remoteMax] = remoteRange
    if (isEmptyRange(remoteRange)) return remoteRange
    if (isEmptyRange(localRange)) return localRange
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
    return [commonMin, commonMax]
  }

  function isEmptyRange(range) {
    const [min, max] = range
    return min > max
  }

  function countIter(iter) {
    let count = 0
    for (const _ of iter) count++
    return count
  }

  function bloomFor(feedId, round, range, extraKeys = []) {
    const filterSize =
      (isEmptyRange(range) ? 2 : estimateMsgCount(range)) + countIter(extraKeys)
    const filter = BloomFilter.create(2 * filterSize, 0.00001)
    if (!isEmptyRange(range)) {
      for (const msg of yieldMsgsIn(feedId, range)) {
        filter.add('' + round + msg.key)
      }
    }
    for (const msgKey of extraKeys) {
      filter.add('' + round + msgKey)
    }
    return filter.saveAsJSON()
  }

  function msgsMissing(feedId, round, range, remoteBloomJSON) {
    if (isEmptyRange(range)) return []
    const remoteFilter = BloomFilter.fromJSON(remoteBloomJSON)
    const missing = []
    for (const msg of yieldMsgsIn(feedId, range)) {
      if (!remoteFilter.has('' + round + msg.key)) {
        missing.push(msg.key)
      }
    }
    return missing
  }

  return {
    localRangeFor: calcRange,
    commonRangeFor,
    isEmptyRange,
    bloomFor,
    msgsMissing,
    yieldMsgsIn,
    getMsgs,
    sink,
  }
}
