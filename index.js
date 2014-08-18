var request = require('request')
var through = require('through2')
var split = require('binary-split')
var once = require('once')
var pump = require('pump')
var concat = require('concat-stream')

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

module.exports = function(dat, cb) {
  latestSeq(dat, function(seq) {
    seq = Math.max(0, seq-1) // sub 1 incase of errors

    if (seq) log('Continuing fetching npm data from seq: %d', seq)

    var url = 'https://skimdb.npmjs.com/registry/_changes?heartbeat=30000&include_docs=true&feed=continuous' + (seq ? '&since=' + seq : '');

    var getAttachments = function(latest, cb) {
      if (!latest.versions) return cb(null, latest)
      var versions = Object.keys(latest.versions)

      var loop = function(err, latest) {
        if (err) return cb(err)

        var version = versions.shift()
        if (!version) {
          log('No more blobs for %s', latest.name)
          return cb(null, latest)
        }

        var filename = latest.name + '-' + version + '.tgz';
        var tgz = latest.versions[version].dist.tarball

        if (!tgz) {
          log('No dist.tarball available for %s (%s)', latest.name, version)
          return tick(loop, null, latest)
        }

        if (latest.blobs && latest.blobs[filename]) return tick(loop, null, latest)

        var next = once(loop)
        var ws = dat.createBlobWriteStream(filename, latest, next)

        log('Downloading %s', tgz)

        pump(request(tgz, {timeout:10*60*1000}), ws, function(err) {
          if (err) next(err)
        })
      }

      loop(null, latest)
    }

    var update = function(err) {
      if (err) {
        log('Error: %s - retrying in 5s', err.message)
        return setTimeout(update, 5000)
      }

      var write = function(data, enc, cb) {
        data = JSON.parse(data)
        var doc = data.doc

        log('Updating %s (seq: %d)', doc._id, data.seq)

        var ondone = function(err, doc) {
          if (err) return cb(err)
          log('Updated %s (version: %d)', doc.key, doc.version)
          cb()
        }

        // dat uses .key
        doc.key = doc._id
        delete doc._id

        // dat reserves .version, and .version shouldn't be on top level of npm docs anyway
        delete doc.version

        // keep the seq around because why not
        doc.couchSeq = data.seq

        var put = function(doc) {
          dat.put(doc, {version:doc.version}, function(err, doc) {
            if (err) return ondone(err)
            getAttachments(doc, ondone)
          })
        }

        dat.get(doc.key, function(err, existing) {
          if (err && !err.notFound) return ondone(err)

          if (!existing) return put(doc)

          log('Previous version for %s (version: %d) found. Updating...', doc.key, existing.version)
          doc.blobs = existing.blobs
          doc.version = existing.version

          // ensure that the old blobs we're fetched (to survive a restart)
          getAttachments(doc, function(err, doc) {
            if (err) return ondone(err)
            put(doc)
          })
        })
      }

      pump(request(url), split(), through.obj(write), update)
    }

    update()
  })

  cb()
}