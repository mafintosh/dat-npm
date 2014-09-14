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
          var versions = Object.keys(doc.versions)

          var loop = function() {
            if (!versions.length) return dat.put(doc, {version:doc.version}, ondone)

            var version = versions.shift()
            var latest = doc.versions[version]
            var filename = latest.name + '-' + version + '.tgz';
            var tgz = doc.versions[version].dist.tarball

            if (!tgz) {
              log('No dist.tarball available for %s (%s)', doc.name, version)
              return loop()
            }

            doc.blobs = doc.blobs || {}
            if (doc.blobs[filename]) return loop()

            request.head(tgz, function(err, response) {
              if (err) return ondone(err)

              doc.blobs[filename] = {
                key: filename,
                size: Number(response.headers['content-length']),
                link: tgz
              }

              loop()
            })

          }

          loop()
        }

        dat.get(doc.key, function(err, existing) {
          if (err && !err.notFound) return ondone(err)

          if (!existing) return put(doc)

          log('Previous version for %s (version: %d) found. Updating...', doc.key, existing.version)
          doc.blobs = existing.blobs
          doc.version = existing.version

          put(doc)
        })
      }

      pump(request(url), split(), through.obj(write), update)
    }

    update()
  })

  cb()
}