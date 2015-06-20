var fs = require('fs')
var request = require('request')
var through = require('through2')
var ndjson = require('ndjson')
var once = require('once')
var pump = require('pump')
var concat = require('concat-stream')
var parallel = require('parallel-transform')
var Dat = require('dat-core')

var dat = Dat('./npm', {createIfMissing: true, valueEncoding: 'json'})
var modules = dat.dataset('modules')

update()

function update (err) {
  if (err) {
    log('Error: %s - retrying in 5s', err.message)
    return setTimeout(update, 5000)
  }

  latestSeq(function (err, seq) {
    if (err) throw err
    
    seq = Math.max(0, seq-1) // sub 1 incase of errors

    if (seq) log('Continuing fetching npm data from seq: %d', seq)

    var url = 'https://skimdb.npmjs.com/registry/_changes?heartbeat=30000&include_docs=true&feed=continuous' + (seq ? '&since=' + seq : '')

    pump(request(url), ndjson.parse(), normalize(), save(), update)
  })
}

function latestSeq (cb) {
  fs.readFile('./seq.json', function (err, buf) {
    if (err && err.code !== 'ENOENT') return cb(err)
    if (!buf) return cb(null, 0)
    cb(null, +JSON.parse(buf).seq)
  })
}

function tick (fn, err, val) {
  process.nextTick(function () {
    fn(err, val)
  })
}

function log (fmt) {
  fmt = '[dat-npm] ' + fmt
  console.log.apply(console, arguments)
}

function normalize () {
  return through.obj(function (data, enc, cb) {
    var doc = data.doc
    if (!doc) return cb()

    // dat uses .key
    doc.key = doc._id
    delete doc._id

    // keep the seq around because why not
    doc.couchSeq = data.seq
      
    modules.get(doc.key, function (err, existing) {
      if (err && !err.notFound) return cb(err)
      if (!existing) return cb(null, doc)

      if (doc._rev > existing._rev) {
        log('Previous version for %s (version: %s) found. Updating to %s...', doc.key, existing._rev, doc._rev)
        cb(null, doc)
      } else {
        log('Already have data for %s version: %s, skipping.', doc.key, doc._rev)
        cb() // nothing to update
      }
    })
  })
}

function save () {
  return through.obj(function (doc, enc, cb) {
    modules.put(doc.key, doc, function (err) {
      if (err) return cb(err)
      fs.writeFile('./seq.json', JSON.stringify({seq: doc.couchSeq}), function (err) {
        if (err) return cb(err)
        log('Updated %s (rev: %s)', doc.key, doc._rev)
        cb()
      })
    })
  })
}
