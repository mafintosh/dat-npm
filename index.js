var request = require('request')
var through = require('through2')
var split = require('binary-split')
var once = require('once')
var pump = require('pump')
var concat = require('concat-stream')
var parallel = require('parallel-transform')

var tick = function(fn, err, val) {
  process.nextTick(function() {
    fn(err, val)
  })
}

var latestSeq = function(dat, ready) {
  // 10 just to make sure - pretty sure we only need 2 (1 in case we hit a schema)
  dat.createChangesReadStream({data:true, decode:true, tail:10}).pipe(concat(function(changes) {
    if (!changes.length) return ready(0)

    var seq = changes
      .map(function(change) {
        return (change.value && change.value.couchSeq) || 0
      })
      .reduce(function(a, b) {
        return Math.max(a, b)
      })

    ready(seq)
  }))
}

var log = function(fmt) {
  fmt = '[dat-npm] '+fmt
  console.log.apply(console, arguments)
}

var addBlobSize = function() {
  return parallel(20, function(data, cb) {
    var blobs = Object.keys(data.blobs || {})
    var i = 0

    var loop = function() {
      if (i >= blobs.length) return cb(null, data)

      var key = blobs[i++]
      var bl = data.blobs[key]

      if (typeof bl.size === 'number') return loop()

      request.head(bl.link, function(err, response) {
        if (err) return cb(err)
        if (response.statusCode === 404) {
          log('404 for blob %s (%s) - removing...', bl.link, data.key)
          delete data.blobs[key]
          return loop()
        }
        if (response.statusCode !== 200) return cb(new Error('bad status code for '+bl.key+' ('+response.statusCode+')'))

        log('Fetched blob size for %s (%s)', bl.link, data.key)
        bl.size = Number(response.headers['content-length'])
        loop()
      })
    }

    loop()
  })
}

var parse = function(dat) {
  return through.obj(function(data, enc, cb) {
    data = JSON.parse(data)
    var doc = data.doc

    // dat uses .key
    doc.key = doc._id
    delete doc._id

    // dat reserves .version, and .version shouldn't be on top level of npm docs anyway
    delete doc.version

    // keep the seq around because why not
    doc.couchSeq = data.seq

    var push = function(doc, updated) {
      var versions = Object.keys((typeof doc.versions === 'object' && doc.versions) || {})

      versions.forEach(function(version) {
        var latest = doc.versions[version]
        var filename = latest.name + '-' + version + '.tgz';
        var tgz = doc.versions[version].dist.tarball

        if (!tgz) {
          log('No dist.tarball available for %s (%s)', doc.name, version)
          return
        }

        doc.blobs = doc.blobs || {}
        if (doc.blobs[filename]) return

        updated = true
        doc.blobs[filename] = {
          key: filename,
          link: tgz
        }
      })

      if (updated) return cb(null, doc)

      log('%s was not updated - skipping', doc.name)
      cb()
    }

    dat.get(doc.key, function(err, existing) {
      if (err && !err.notFound) return cb(err)
      if (!existing) return push(doc, true)

      log('Previous version for %s (version: %d) found. Updating...', doc.key, existing.version)
      doc.blobs = existing.blobs
      doc.version = existing.version

      push(doc, false)
    })
  })
}

var save = function(dat) {
  return through.obj(function(doc, enc, cb) {
    dat.put(doc, {version:doc.version}, function(err, doc) {
      if (err) return cb(err)
      log('Updated %s (version: %d)', doc.key, doc.version)
      cb()
    })
  })
}

module.exports = function(dat, cb) {
  var update = function(err) {
    if (err) {
      log('Error: %s - retrying in 5s', err.message)
      return setTimeout(update, 5000)
    }

    latestSeq(dat, function(seq) {
      seq = Math.max(0, seq-1) // sub 1 incase of errors

      if (seq) log('Continuing fetching npm data from seq: %d', seq)

      var url = 'https://skimdb.npmjs.com/registry/_changes?heartbeat=30000&include_docs=true&feed=continuous' + (seq ? '&since=' + seq : '');

      pump(request(url), split(), parse(dat), addBlobSize(dat), save(dat), update)
    })
  }

  update()
  cb()
}