#!/usr/bin/env node

// haha
// process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

var Dat = require('dat');
var request = require('request')
var series = require('run-series')
var through = require('through2');
var path = require('path');
var ChangesStream = require('changes-stream')
var flat = require('flat-file-db')
var log = require('single-line-log').stdout
var optimist = require('optimist')
.usage('Usage: $0 [folder]')
.option('p', {
  alias: 'port',
  describe: 'set the port to listen on'
});

var argv = optimist.argv;
var folder = argv._[0];

if (!folder) {
  optimist.showHelp();
  process.exit(1);
}

var dat = new Dat(folder, {port: process.env.PORT || argv.port, serve: true}, function(err) {
  if (err) throw err;
  
  var db = flat.sync(path.join(folder, 'sync.db'))
  var seq = db.get('seq')
  
  if (seq) console.log('last seq', seq)
  
  // haha
  setInterval(function() {
    if (seq) db.put('seq', seq)
  }, 10000)
  
  update()
  
  function update() {
    
    console.log('creating changes stream...')

    var changes = new ChangesStream({
      db: 'https://fullfatdb.npmjs.com/registry',
      include_docs: true,
      since: seq
    });
    
    var count = 0
    
    var fetcher = through.obj({ highWaterMark: 50 }, function(data, _, cb) {
      setTimeout(cb, 1000)
      // var doc = data.doc
   //    
   //    // dat uses .id
   //    doc.id = doc._id
   //    delete doc._id
   //    
   //    // keep the seq around because why not
   //    doc.couchSeq = seq = data.seq
   //    
   //    dat.put(doc, function(err, latest) {
   //      if (err) throw err
   //      
   //      var versions = Object.keys(latest.versions)
   //      
   //      // fetch all attachments
   //      var fns = versions.map(function(version) {
   //        var tgz = latest.versions[version].dist.tarball
   //        if (!tgz) return function(cb) { setImmediate(cb) }
   //        return function(cb) {
   //          var filename = latest.name + '-' + version + '.tgz'
   //          
   //          var ws = dat.createBlobWriteStream(filename, latest, function(err, updated) {
   //            if (err) return cb(err)
   //            latest = updated
   //            cb()
   //          })
   //          
   //          console.log('tgz GET', tgz)
   //          
   //          request(tgz).pipe(ws)
   //        }
   //      })
   //      
   //      series(fns, function(err, results) {
   //        if (err) console.error('GET ERR', err)
   //        console.log(++count, [latest.id, latest.version])
   //        cb()
   //      })
   //      
   //    })
    })
    
    changes.pipe(fetcher)
  }
})