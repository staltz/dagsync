const toPull = require('push-stream-to-pull-stream')
const pull = require('pull-stream')
const makeDebug = require('debug')
const getSeverity = require('ssb-network-errors')
const dagSyncAlgorithm = require('./algorithm')
const DAGSyncStream = require('./stream')

function isMuxrpcMissingError(err, namespace, methodName) {
  const jsErrorMessage = `method:${namespace},${methodName} is not in list of allowed methods`
  const goErrorMessage = `muxrpc: no such command: ${namespace}.${methodName}`
  return err.message === jsErrorMessage || err.message === goErrorMessage
}

module.exports = function makeDagSyncPlugin(opts) {
  return {
    name: opts.name,
    manifest: {
      connect: 'duplex',
      request: 'sync',
    },
    permissions: {
      anonymous: {
        allow: ['connect'],
      },
    },
    init(ssb, config) {
      const debug = makeDebug(`ssb:${opts.name}`)
      const algo = dagSyncAlgorithm({ ...opts, ssb })

      const streams = []
      function createStream(remoteId, isClient) {
        // prettier-ignore
        debug('Opening a stream with remote %s %s', isClient ? 'server' : 'client', remoteId)
        const stream = new DAGSyncStream(ssb.id, debug, algo)
        streams.push(stream)
        return stream
      }

      ssb.on('rpc:connect', function onDagSyncRPCConnect(rpc, isClient) {
        if (rpc.id === ssb.id) return // ssb-client connecting to ssb-server
        if (!isClient) return
        const local = toPull.duplex(createStream(rpc.id, true))

        const remote = rpc[opts.name].connect((networkError) => {
          if (networkError && getSeverity(networkError) >= 3) {
            if (isMuxrpcMissingError(networkError, opts.name, 'connect')) {
              console.warn(`peer ${rpc.id} does not support dagsync connect`)
              // } else if (isReconnectedError(networkError)) { // TODO: bring back
              // Do nothing, this is a harmless error
            } else {
              console.error(`rpc.${opts.name}.connect exception:`, networkError)
            }
          }
        })

        pull(local, remote, local)
      })

      function connect() {
        // `this` refers to the remote peer who called this muxrpc API
        return toPull.duplex(createStream(this.id, false))
      }

      function request(id) {
        for (const stream of streams) {
          stream.request(id)
        }
      }
      return {
        connect,
        request,
      }
    },
  }
}
