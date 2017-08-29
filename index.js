var fs = require('fs')
var request = require('request')
var through = require('through2')
var pump = require('pump')
var ndjson = require('ndjson')
var concat = require('concat-stream')
var parallel = require('parallel-transform')
var hyperdb = require('hyperdb')
var hyperdiscovery = require('hyperdiscovery')
var db = hyperdb('./npm.db', {valueEncoding: 'json'})
 
update()

function update (err) {
  if (err) {
    log('Error: %s - retrying in 5s', err.message)
    return setTimeout(update, 5000)
  }

  latestSeq(function (err, seq) {
    if (err) throw err
    
    seq = Math.max(0, seq - 1) // sub 1 incase of errors

    if (seq) log('Continuing fetching npm data from seq: %d', seq)

    var url = 'https://skimdb.npmjs.com/registry/_changes?heartbeat=30000&include_docs=true&feed=continuous' + (seq ? '&since=' + seq : '')

    pump(request(url), ndjson.parse(), save(), update)
  })
}

function latestSeq (cb) {
  db.get('/latest-seq', function (err, val) {
    if (err || !val) return cb(null, 0)
    var seq = val[0].value
    cb(null, seq)
  })
  
  db.on('ready', function () {
    console.log('sharing hypercore', db.key.toString('hex'))
    db.discovery = hyperdiscovery(db, {live: true})
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
    var versions = {}
    Object.keys(doc.versions).map(function (v) {
      var thisVer = doc.versions[v]
      versions[v] = {
        dependencies: thisVer.dependencies,
        optionalDependencies: thisVer.optionalDependencies,
        devDependencies: thisVer.devDependencies
      }
    })
    db.put('/modules/' + key, versions, function (err) {
      if (err) return cb(err)
      log('wrote /modules/' + key + '=' + versions + ', seq=' + data.seq)
      db.put('/latest-seq', data.seq, cb)
    })
  })
}
