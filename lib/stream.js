const Pipeable = require('push-stream/pipeable')

class DAGSyncStream extends Pipeable {
  #myId
  #debug
  #algo
  #requested
  #commonRange
  #receivableMsgs
  #sendableMsgs

  constructor(localId, debug, algo) {
    super()
    this.paused = false // TODO: should we start as paused=true?
    this.ended = false
    this.source = this.sink = null
    this.#myId = localId.slice(0, 6)
    this.#debug = debug
    this.#algo = algo
    this.#requested = new Set()
    this.#commonRange = new Map() // id => common range with remote peer
    this.#receivableMsgs = new Map() // id => Set<msgIDs>
    this.#sendableMsgs = new Map() // id => Set<msgIDs>
  }

  // public API
  request(id) {
    this.#requested.add(id)
    this.resume()
  }

  #canSend() {
    return this.sink && !this.sink.paused && !this.ended
  }

  #updateSendableMsgs(id, msgs) {
    const set = this.#sendableMsgs.get(id) ?? new Set()
    for (const msg of msgs) {
      set.add(msg)
    }
    this.#sendableMsgs.set(id, set)
  }

  #updateReceivableMsgs(id, msgs) {
    const set = this.#receivableMsgs.get(id) ?? new Set()
    for (const msg of msgs) {
      set.add(msg)
    }
    this.#receivableMsgs.set(id, set)
  }

  #sendLocalRange(id) {
    const localRange = this.#algo.localRangeFor(id)
    // prettier-ignore
    this.#debug('%s Stream OUT: send local range %o for %s', this.#myId, localRange, id)
    this.sink.write({ id, phase: 1, payload: localRange })
  }

  #sendCommonRange(id, remoteRange) {
    // prettier-ignore
    this.#debug('%s Stream IN: received remote range %o for %s', this.#myId, remoteRange, id)
    const commonRange = this.#algo.commonRangeFor(id, remoteRange)
    this.#commonRange.set(id, commonRange)
    this.sink.write({ id, phase: 2, payload: commonRange })
    // prettier-ignore
    this.#debug('%s Stream OUT: send common range %o for %s', this.#myId, commonRange, id)
  }

  sendInitBloomReq(id, commonRange) {
    // prettier-ignore
    this.#debug('%s Stream IN: received common range %o for %s', this.#myId, commonRange, id)
    this.#commonRange.set(id, commonRange)
    const localBloom0 = this.#algo.bloomFor(id, 0, commonRange)
    this.sink.write({ id, phase: 3, payload: localBloom0 })
    this.#debug('%s Stream OUT: send bloom round 0 for %s', this.#myId, id)
  }

  #sendInitBloomRes(id, remoteBloom) {
    // prettier-ignore
    this.#debug('%s Stream IN: received bloom round 0 for %s', this.#myId, id)
    const commonRange = this.#commonRange.get(id)
    const msgIDsForThem = this.#algo.msgsMissing(
      id,
      0,
      commonRange,
      remoteBloom
    )
    this.#updateSendableMsgs(id, msgIDsForThem)
    const localBloom = this.#algo.bloomFor(id, 0, commonRange)
    this.sink.write({
      id,
      phase: 4,
      payload: { bloom: localBloom, msgIDs: msgIDsForThem },
    })
    // prettier-ignore
    this.#debug('%s Stream OUT: send bloom round 0 plus msgIDs in %s', this.#myId, id)
  }

  #sendBloomReq(id, phase, round, remoteBloom, msgIDsForMe) {
    // prettier-ignore
    this.#debug('%s Stream IN: received bloom round %s plus msgIDs in %s: %o', this.#myId, round-1, id, msgIDsForMe)
    const commonRange = this.#commonRange.get(id)
    this.#updateReceivableMsgs(id, msgIDsForMe)
    const msgIDsForThem = this.#algo.msgsMissing(
      id,
      round - 1,
      commonRange,
      remoteBloom
    )
    this.#updateSendableMsgs(id, msgIDsForThem)
    const localBloom = this.#algo.bloomFor(
      id,
      round,
      commonRange,
      this.#receivableMsgs.get(id)
    )
    this.sink.write({
      id,
      phase,
      payload: { bloom: localBloom, msgIDs: msgIDsForThem },
    })
    // prettier-ignore
    this.#debug('%s Stream OUT: send bloom round %s plus msgIDs in %s: %o', this.#myId, round, id, msgIDsForThem)
  }

  #sendBloomRes(id, phase, round, remoteBloom, msgIDsForMe) {
    // prettier-ignore
    this.#debug('%s Stream IN: received bloom round %s plus msgIDs in %s: %o', this.#myId, round, id, msgIDsForMe)
    const commonRange = this.#commonRange.get(id)
    this.#updateReceivableMsgs(id, msgIDsForMe)
    const msgIDsForThem = this.#algo.msgsMissing(
      id,
      round,
      commonRange,
      remoteBloom
    )
    this.#updateSendableMsgs(id, msgIDsForThem)
    const localBloom = this.#algo.bloomFor(
      id,
      round,
      commonRange,
      this.#receivableMsgs.get(id)
    )
    this.sink.write({
      id,
      phase,
      payload: { bloom: localBloom, msgIDs: msgIDsForThem },
    })
    // prettier-ignore
    this.#debug('%s Stream OUT: send bloom round %s plus msgIDs in %s: %o', this.#myId, round, id, msgIDsForThem)
  }

  #sendLastBloomRes(id, phase, round, remoteBloom, msgIDsForMe) {
    // prettier-ignore
    this.#debug('%s Stream IN: received bloom round %s plus msgIDs in %s: %o', this.#myId, round, id, msgIDsForMe)
    const commonRange = this.#commonRange.get(id)
    this.#updateReceivableMsgs(id, msgIDsForMe)
    const msgIDsForThem = this.#algo.msgsMissing(
      id,
      round,
      commonRange,
      remoteBloom
    )
    this.#updateSendableMsgs(id, msgIDsForThem)
    this.sink.write({ id, phase, payload: msgIDsForThem })
    // prettier-ignore
    this.#debug('%s Stream OUT: send msgIDs in %s: %o', this.#myId, id, msgIDsForThem)
  }

  #sendMissingMsgsReq(id, msgIDsForMe) {
    // prettier-ignore
    this.#debug('%s Stream IN: received msgIDs in %s: %o', this.#myId, id, msgIDsForMe)
    this.#updateReceivableMsgs(id, msgIDsForMe)
    const msgIDs = this.#sendableMsgs.has(id)
      ? [...this.#sendableMsgs.get(id)]
      : []
    this.#algo.getMsgs(msgIDs, (err, msgs) => {
      // prettier-ignore
      if (err) throw new Error('sendMissingMsgsReq failed because getMsgs failed', {cause: err})
      // prettier-ignore
      this.#debug('%s Stream OUT: send %s msgs in %s', this.#myId, msgs.length, id)
      this.sink.write({ id, phase: 9, payload: msgs })
    })
  }

  #sendMissingMsgsRes(id, msgsForMe) {
    // prettier-ignore
    this.#debug('%s Stream IN: received %s msgs in %s', this.#myId, msgsForMe.length, id)
    const msgIDs = this.#sendableMsgs.has(id)
      ? [...this.#sendableMsgs.get(id)]
      : []
    this.#algo.getMsgs(msgIDs, (err, msgs) => {
      // prettier-ignore
      if (err) throw new Error('sendMissingMsgsReq failed because getMsgs failed', {cause: err})
      // prettier-ignore
      this.#debug('%s Stream OUT: send %s msgs in %s', this.#myId, msgs.length, id)
      this.sink.write({ id, phase: 10, payload: msgs })
    })

    this.#requested.delete(id)
    this.#commonRange.delete(id)
    this.#receivableMsgs.delete(id)
    this.#sendableMsgs.delete(id)
    this.#algo.sink(msgsForMe, (err) => {
      // prettier-ignore
      if (err) throw new Error('sendMissingMsgsRes failed because sink failed', {cause: err})
    })
  }

  #consumeMissingMsgs(id, msgsForMe) {
    // prettier-ignore
    this.#debug('%s Stream IN: received %s msgs in %s', this.#myId, msgsForMe.length, id)

    this.#requested.delete(id)
    this.#commonRange.delete(id)
    this.#receivableMsgs.delete(id)
    this.#sendableMsgs.delete(id)
    this.#algo.sink(msgsForMe, (err) => {
      // prettier-ignore
      if (err) throw new Error('sendMissingMsgsRes failed because sink failed', {cause: err})
    })
  }

  #sendMsgsInLocalRange(id) {
    this.#debug('%s Stream IN: received empty range for %s', this.#myId, id)
    const localRange = this.#algo.localRangeFor(id)
    const msgVals = []
    for (const msg of this.#algo.yieldMsgsIn(id, localRange)) {
      msgVals.push(msg.value)
    }
    // prettier-ignore
    this.#debug('%s Stream OUT: send %s msgs in %s', this.#myId, msgVals.length, id)
    this.sink.write({ id, phase: 10, payload: msgVals })
  }

  // as a source
  resume() {
    if (!this.sink || this.sink.paused) return

    for (const id of this.#requested) {
      if (!this.#canSend()) return
      this.#sendLocalRange(id)
    }
  }

  // as a sink
  write(data) {
    const { id, phase, payload } = data

    switch (phase) {
      case 1: {
        if (this.#algo.isEmptyRange(payload)) return this.#sendMsgsInLocalRange(id)
        return this.#sendCommonRange(id, payload)
      }
      case 2: {
        if (this.#algo.isEmptyRange(payload)) return this.#sendMsgsInLocalRange(id)
        return this.sendInitBloomReq(id, payload)
      }
      case 3: {
        return this.#sendInitBloomRes(id, payload)
      }
      case 4: {
        const { bloom, msgIDs } = payload
        return this.#sendBloomReq(id, phase + 1, 1, bloom, msgIDs)
      }
      case 5: {
        const { bloom, msgIDs } = payload
        return this.#sendBloomRes(id, phase + 1, 1, bloom, msgIDs)
      }
      case 6: {
        const { bloom, msgIDs } = payload
        return this.#sendBloomReq(id, phase + 1, 2, bloom, msgIDs)
      }
      case 7: {
        const { bloom, msgIDs } = payload
        return this.#sendLastBloomRes(id, phase + 1, 2, bloom, msgIDs)
      }
      case 8: {
        return this.#sendMissingMsgsReq(id, payload)
      }
      case 9: {
        return this.#sendMissingMsgsRes(id, payload)
      }
      case 10: {
        return this.#consumeMissingMsgs(id, payload)
      }
    }

    this.#debug('Stream IN: unknown %o', data)
  }

  // as a source
  abort(err) {
    this.ended = true
    if (this.source && !this.source.ended) this.source.abort(err)
    if (this.sink && !this.sink.ended) this.sink.end(err)
  }

  // as a sink
  end(err) {
    this.ended = true
    if (this.source && !this.source.ended) this.source.abort(err)
    if (this.sink && !this.sink.ended) this.sink.end(err)
  }
}

module.exports = DAGSyncStream
