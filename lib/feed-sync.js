const p = require('util').promisify
const dagSyncPlugin = require('./plugin')

module.exports = dagSyncPlugin('feedSync', (ssb, config) => {
  const limit = config.feedSync?.limit ?? 1000

  function* take(n, iter) {
    if (n === 0) return
    let i = 0
    for (const item of iter) {
      yield item
      if (++i >= n) break
    }
  }

  return {
    haveRange(feedId) {
      let minSeq = Number.MAX_SAFE_INTEGER
      let maxSeq = 0
      ssb.db.forEach((msg) => {
        if (msg.value.author === feedId) {
          minSeq = Math.min(minSeq, msg.value.sequence)
          maxSeq = Math.max(maxSeq, msg.value.sequence)
        }
      })
      return [minSeq, maxSeq]
    },

    wantRange(feedId, localHaveRange, remoteHaveRange) {
      const [minLocalHave, maxLocalHave] = localHaveRange
      const [minRemoteHave, maxRemoteHave] = remoteHaveRange
      if (maxRemoteHave <= maxLocalHave) return [1, 0]
      const maxWant = maxRemoteHave
      const size = Math.max(maxWant - maxLocalHave, limit)
      const minWant = Math.max(maxWant - size, maxLocalHave + 1, minRemoteHave)
      return [minWant, maxWant]
    },

    estimateMsgCount(range) {
      const [minSeq, maxSeq] = range
      const estimate = maxSeq - minSeq + 1
      if (estimate > 1000) return 1000
      else if (estimate < 5) return 5
      else return estimate
    },

    *yieldMsgsIn(feedId, range) {
      const [minSeq, maxSeq] = range
      for (const msg of ssb.db.filterAsIterator((msg) => !!msg)) {
        const { author, sequence } = msg.value
        if (author === feedId && sequence >= minSeq && sequence <= maxSeq) {
          yield msg
        }
      }
    },

    getMsgs(msgKeys, cb) {
      const msgs = []
      for (const msgKey of msgKeys) {
        const msg = ssb.db.get(msgKey)
        if (msg) msgs.push(msg)
      }
      cb(null, msgs)
    },

    async sink(newMsgVals, cb) {
      newMsgVals.sort((a, b) => a.sequence - b.sequence) // mutation
      const feedId = newMsgVals[0].author
      const isRelevantMsg = (msg) => msg?.value?.author === feedId

      // Find max sequence in the database
      let oldLastSequence = 0
      let oldCount = 0
      for (const msg of ssb.db.filterAsIterator(isRelevantMsg)) {
        oldCount += 1
        oldLastSequence = Math.max(oldLastSequence, msg.value.sequence)
      }

      const isContinuation = newMsgVals[0].sequence === oldLastSequence + 1
      // Refuse creating holes in the feed
      if (!isContinuation && newMsgVals.length < limit) {
        console.error(
          `feedSync failed to persist msgs for ${feedId} because ` +
            'they are not a continuation, and not enough messages'
        )
        return cb()
      }

      // Delete old messages in the database
      if (isContinuation) {
        // Delete just enough msgs to make room for the new ones
        const N = Math.max(0, oldCount + newMsgVals.length - limit)
        for (const msg of take(N, ssb.db.filterAsIterator(isRelevantMsg))) {
          await p(ssb.db.del)(msg.key)
        }
      } else {
        // Delete all the old ones
        for (const msg of ssb.db.filterAsIterator(isRelevantMsg)) {
          await p(ssb.db.del)(msg.key)
        }
      }

      // Add new messages
      for (const msgVal of newMsgVals) {
        await p(ssb.db.add)(msgVal)
      }
      cb()
    },
  }
})
