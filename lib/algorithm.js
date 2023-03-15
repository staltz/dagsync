const { BloomFilter } = require('bloom-filters')

module.exports = function dagSyncAlgorithm(opts = {}) {
  const {
    haveRange,
    wantRange,
    estimateMsgCount,
    yieldMsgsIn,
    commit,
  } = opts
  if (typeof haveRange !== 'function') {
    throw new Error('function haveRange is required')
  }
  if (typeof wantRange !== 'function') {
    throw new Error('function wantRange is required')
  }
  if (typeof estimateMsgCount !== 'function') {
    throw new Error('function estimateMsgCount is required')
  }
  if (typeof yieldMsgsIn !== 'function') {
    throw new Error('function yieldMsgsIn is required')
  }
  if (typeof commit !== 'function') {
    throw new Error('function commit is required')
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

  function betterWantRange(feedId, localHaveRange, remoteHaveRange) {
    if (isEmptyRange(remoteHaveRange)) return [1, 0]
    else return wantRange(feedId, localHaveRange, remoteHaveRange)
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
    haveRange,
    wantRange: betterWantRange,
    isEmptyRange,
    bloomFor,
    msgsMissing,
    yieldMsgsIn,
    commit,
  }
}
