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
var tarballs = dat.dataset('tarballs')

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

    pump(request(url), ndjson.parse(), normalize(), addBlobSize(), save(), update)
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
      if (!existing) return push(doc)

      if (doc._rev > existing._rev) {
        log('Previous version for %s (version: %s) found. Updating to %s...', doc.key, existing._rev, doc._rev)
        push(doc)
      } else {
        log('Already have data for %s version: %s, skipping.', doc.key, doc._rev)
        cb() // nothing to update
      }
    })

    function push (doc) {
      var versions = Object.keys((typeof doc.versions === 'object' && doc.versions) || {})
      var tarballs = []

      versions.forEach(function (version) {
        var latest = doc.versions[version]
        var filename = latest.name + '-' + version + '.tgz'
        var tgz = doc.versions[version].dist.tarball

        if (!tgz) {
          log('No dist.tarball available for %s (%s)', doc.name, version)
          return
        }

        tarballs.push({
          key: filename,
          link: tgz
        })
      })

      cb(null, {module: doc, tarballs: tarballs})
    }
  })
}

function addBlobSize () {
  return parallel(20, function (data, cb) {
    var blobs = data.tarballs
    var modified = []
    var i = 0

    loop()

    function loop () {
      if (i >= blobs.length) {
        data.tarballs = modified
        return cb(null, data)
      }

      var bl = blobs[i++]

      if (typeof bl.size === 'number') return loop()

      tarballs.get(bl.key, function (err, existing) {
        if (err && !err.notFound) return cb(err)
        if (existing) {
          log('Already have size for %s - skipping...', bl.key)
          return loop()
        }
        request.head(bl.link, function (err, response) {
          if (err) return cb(err)
          if (response.statusCode === 404) {
            log('404 for blob %s (%s) - removing...', bl.link, data.key)
            return loop()
          }
          if (response.statusCode !== 200) return cb(new Error('bad status code for '+bl.key+' ('+response.statusCode+')'))

          log('Fetched blob size for %s (%s)', bl.link, data.module.key)
          bl.size = Number(response.headers['content-length'])
          modified.push(bl)
          loop()
        })
      })
    }
  })
}

function save () {
  return through.obj(function (data, enc, cb) {
    modules.put(data.module.key, data.module, function (err) {
      if (err) return cb(err)
      tarballs.batch(data.tarballs.map(function (t) {
        return {
          type: 'put',
          content: 'file',
          key: t.key,
          value: {
            link: t.link,
            size: t.size
          }
        }
      }), function (err) {
        if (err) return cb(err)
        fs.writeFile('./seq.json', JSON.stringify({seq: data.module.couchSeq}), function (err) {
          if (err) return cb(err)
          log('Updated %s (rev: %s)', data.module.key, data.module._rev)
          cb()
        })
      })
    })
  })
}
