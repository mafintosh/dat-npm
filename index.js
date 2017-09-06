var fs = require('fs')
var request = require('request')
var through = require('through2')
var pump = require('pump')
var ndjson = require('ndjson')
var concat = require('concat-stream')
var parallel = require('parallel-transform')
var hyperdb = require('hyperdb')
var hyperdrive = require('hyperdrive')
var hyperdiscovery = require('hyperdiscovery')
var minify = require('minify-registry-metadata')
 
module.exports = function (keys, cb) {
  if (typeof keys === 'function') {
    cb = keys
    keys = null
  }
  var meta
  var tarballs = hyperdrive('./npm-tarballs.db', keys && keys.tarballs, {live: true})
  tarballs.on('ready', function () {
    var tarballSwarm = hyperdiscovery(tarballs)
    meta = hyperdb('./npm-meta.db', keys && keys.meta, {sparse: true, valueEncoding: 'json'})
    meta.on('ready', function () {
      var metaSwarm = hyperdiscovery(meta, {live: true})
      meta.swarm = metaSwarm
      tarballs.swarm = tarballSwarm
      cb(null, {meta: meta, tarballs: tarballs, startUpdating: startUpdating})
    })    
  })

  function startUpdating (err) {
    if (err) {
      log('Error updating: %s - retrying in 5s', err.message)
      return setTimeout(startUpdating, 5000)
    }

    latestSeq(function (err, seq) {
      if (err) throw err
    
      seq = Math.max(0, seq - 1) // sub 1 incase of errors

      if (seq) log('Continuing fetching npm data from seq: %d', seq)

      var url = 'https://skimdb.npmjs.com/registry/_changes?heartbeat=30000&include_docs=true&feed=continuous' + (seq ? '&since=' + seq : '')

      pump(request(url), ndjson.parse(), save(), startUpdating)
    })
  }

  function latestSeq (cb) {
    meta.get('/latest-seq', function (err, val) {
      if (err || !val) return cb(null, 0)
      var seq = val[0].value
      cb(null, seq)
    })
  }

  function tick (fn, err, val) {
    process.nextTick(function () {
      fn(err, val)
    })
  }

  function log (fmt) {
    fmt = '[dat-npm] ' + fmt
    console.error.apply(console, arguments)
  }

  function save () {
    return through.obj(function (data, enc, cb) {
      var doc = data.doc
      if (!doc) return cb()
      if (data.id.match(/^_design\//)) return cb()
      var key = doc._id
      if (doc._deleted) {
        // TODO
        return cb()
      }
      var metadata = minify(doc)
      meta.put('/modules/' + key, metadata, function (err) {
        if (err) return cb(err)
        log('wrote /modules/' + key + ', seq=' + data.seq)
        meta.put('/latest-seq', data.seq, cb)
      })
    })
  }
}