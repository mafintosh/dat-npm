var fs = require('fs')
var path = require('path')
var crypto = require('crypto')
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
var parallel = require('run-parallel')
var mkdirp = require('mkdirp')
 
var PARALLEL = 1024

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
      var tarballs = Object.keys(metadata.versions).map(function (v) {
        var dist = metadata.versions[v].dist
        return {
          filename: path.basename(dist.tarball),
          url: dist.tarball
        }
      })
      downloadTarballs(tarballs, function (err) {
        if (err) return cb(err)
        meta.put('/modules/' + key, metadata, function (err) {
          if (err) return cb(err)
          log('wrote /modules/' + key + ', seq=' + data.seq)
          meta.put('/latest-seq', data.seq, cb)
        })
      })
    })
  }
  
  function downloadTarballs (items, done) {
    var fns = items.map(function (i) {
      return function (cb) {
        log('GET', i.url)
        var filename = module.exports.hashFilename(i.filename)
        var r = request(i.url)
        r.on('response', function (re) {
          if (re.statusCode === 404) {
            log('404 ' + i.url)
            return cb() // ignore 404s
          }
          if (re.statusCode > 299) {
            return re.pipe(concat(function (resp) {
              // https://github.com/npm/registry/issues/213
              if (resp.toString().match('Error fetching package from tmp remote')) {
                return cb(null) // ignore this error for now
              }
              return cb(new Error('Status: ' + re.statusCode + ' ' + i.url))
            }))
          }
          var ws = tarballs.createWriteStream(filename)
          pump(re, ws, function (err) {
            if (err) {
              err.errType = 'streamPumpErr'
              return cb(err)
            }
            cb(null)
          })
        })
      }
    })
    parallel(fns, done)
  }
}

// only needed until hyperdb lands in hyperdrive
module.exports.hashFilename = function (filename) {
  var h = crypto.createHash('sha256').update(filename).digest('hex')
  return `${h.slice(0, 2)}/${h.slice(2, 4)}/${h.slice(4, 6)}/${h.slice(6, 8)}/${h.slice(8)}`  
}
