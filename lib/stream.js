const Pipeable = require('push-stream/pipeable')

class DAGSyncStream extends Pipeable {
  #myId
  #debug
  #algo
  #requested
  #remoteHave
  #remoteWant
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
    this.#remoteHave = new Map() // id => have-range by remote peer
    this.#remoteWant = new Map() // id => want-range by remote peer
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

  #sendLocalHave(id) {
    const localHaveRange = this.#algo.haveRange(id)
    // prettier-ignore
    this.#debug('%s Stream OUT: send local have-range %o for %s', this.#myId, localHaveRange, id)
    this.sink.write({ id, phase: 1, payload: localHaveRange })
  }

  #sendLocalHaveAndWant(id, remoteHaveRange) {
    // prettier-ignore
    this.#debug('%s Stream IN: received remote have-range %o for %s', this.#myId, remoteHaveRange, id)
    this.#remoteHave.set(id, remoteHaveRange)
    const haveRange = this.#algo.haveRange(id)
    const wantRange = this.#algo.wantRange(id, haveRange, remoteHaveRange)
    // prettier-ignore
    this.#debug('%s Stream OUT: send local have-range %o and want-range %o for %s', this.#myId, haveRange, wantRange, id)
    this.sink.write({ id, phase: 2, payload: { haveRange, wantRange } })
  }

  #sendLocalWantAndInitBloom(id, remoteHaveRange, remoteWantRange) {
    // prettier-ignore
    this.#debug('%s Stream IN: received remote have-range %o and want-range %o for %s', this.#myId, remoteHaveRange, remoteWantRange, id)
    this.#remoteHave.set(id, remoteHaveRange)
    this.#remoteWant.set(id, remoteWantRange)
    const haveRange = this.#algo.haveRange(id)
    const wantRange = this.#algo.wantRange(id, haveRange, remoteHaveRange)
    const localBloom0 = this.#algo.bloomFor(id, 0, remoteWantRange)
    this.sink.write({
      id,
      phase: 3,
      payload: { bloom: localBloom0, wantRange },
    })
    // prettier-ignore
    this.#debug('%s Stream OUT: send local want-range %o and bloom round 0 for %s', this.#myId, wantRange, id)
  }

  #sendInitBloomRes(id, remoteWantRange, remoteBloom) {
    // prettier-ignore
    this.#debug('%s Stream IN: received remote want-range %o and bloom round 0 for %s', this.#myId, remoteWantRange, id)
    this.#remoteWant.set(id, remoteWantRange)
    const msgIDsForThem = this.#algo.msgsMissing(
      id,
      0,
      remoteWantRange,
      remoteBloom
    )
    this.#updateSendableMsgs(id, msgIDsForThem)
    const localBloom = this.#algo.bloomFor(id, 0, remoteWantRange)
    this.sink.write({
      id,
      phase: 4,
      payload: { bloom: localBloom, msgIDs: msgIDsForThem },
    })
    // prettier-ignore
    this.#debug('%s Stream OUT: send bloom round 0 plus msgIDs in %s: %o', this.#myId, id, msgIDsForThem)
  }

  #sendBloomReq(id, phase, round, remoteBloom, msgIDsForMe) {
    // prettier-ignore
    this.#debug('%s Stream IN: received bloom round %s plus msgIDs in %s: %o', this.#myId, round-1, id, msgIDsForMe)
    const remoteWantRange = this.#remoteWant.get(id)
    this.#updateReceivableMsgs(id, msgIDsForMe)
    const msgIDsForThem = this.#algo.msgsMissing(
      id,
      round - 1,
      remoteWantRange,
      remoteBloom
    )
    this.#updateSendableMsgs(id, msgIDsForThem)
    const localBloom = this.#algo.bloomFor(
      id,
      round,
      remoteWantRange,
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
    const remoteWantRange = this.#remoteWant.get(id)
    this.#updateReceivableMsgs(id, msgIDsForMe)
    const msgIDsForThem = this.#algo.msgsMissing(
      id,
      round,
      remoteWantRange,
      remoteBloom
    )
    this.#updateSendableMsgs(id, msgIDsForThem)
    const localBloom = this.#algo.bloomFor(
      id,
      round,
      remoteWantRange,
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
    const remoteWantRange = this.#remoteWant.get(id)
    this.#updateReceivableMsgs(id, msgIDsForMe)
    const msgIDsForThem = this.#algo.msgsMissing(
      id,
      round,
      remoteWantRange,
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
    this.#remoteHave.delete(id)
    this.#remoteWant.delete(id)
    this.#receivableMsgs.delete(id)
    this.#sendableMsgs.delete(id)
    if (msgsForMe.length === 0) return
    this.#algo.commit(msgsForMe, (err) => {
      // prettier-ignore
      if (err) throw new Error('sendMissingMsgsRes failed because sink failed', {cause: err})
    })
  }

  #consumeMissingMsgs(id, msgsForMe) {
    // prettier-ignore
    this.#debug('%s Stream IN: received %s msgs in %s', this.#myId, msgsForMe.length, id)

    this.#requested.delete(id)
    this.#remoteHave.delete(id)
    this.#remoteWant.delete(id)
    this.#receivableMsgs.delete(id)
    this.#sendableMsgs.delete(id)
    if (msgsForMe.length === 0) return
    this.#algo.commit(msgsForMe, (err) => {
      // prettier-ignore
      if (err) throw new Error('sendMissingMsgsRes failed because sink failed', {cause: err})
    })
  }

  #sendMsgsInRemoteWant(id, remoteWantRange) {
    const msgVals = []
    for (const msg of this.#algo.yieldMsgsIn(id, remoteWantRange)) {
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
      this.#sendLocalHave(id)
    }
  }

  // as a sink
  write(data) {
    const { id, phase, payload } = data

    switch (phase) {
      case 1: {
        return this.#sendLocalHaveAndWant(id, payload)
      }
      case 2: {
        const { haveRange, wantRange } = payload
        if (this.#algo.isEmptyRange(haveRange)) {
          // prettier-ignore
          this.#debug('%s Stream IN: received remote have-range %o and want-range %o for %s', this.#myId, haveRange, wantRange, id)
          return this.#sendMsgsInRemoteWant(id, wantRange)
        } else {
          return this.#sendLocalWantAndInitBloom(id, haveRange, wantRange)
        }
      }
      case 3: {
        const { wantRange, bloom } = payload
        const haveRange = this.#remoteHave.get(id)
        if (haveRange && this.#algo.isEmptyRange(haveRange)) {
          // prettier-ignore
          this.#debug('%s Stream IN: received remote want-range want-range %o and remember empty have-range %o for %s', this.#myId, wantRange, haveRange, id)
          return this.#sendMsgsInRemoteWant(id, wantRange)
        } else {
          return this.#sendInitBloomRes(id, wantRange, bloom)
        }
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
