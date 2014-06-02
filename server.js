#!/usr/bin/env node

var Dat = require('dat');
var request = require('request');
var parallel = require('run-parallel');
var through = require('through2');
var path = require('path');
var flat = require('flat-file-db');
var log = require('single-line-log').stdout;
var split = require('binary-split');

var optimist = require('optimist')
  .usage('Usage: $0 [folder]')
  .option('p', {
    alias: 'port',
    describe: 'set the port to listen on'
  });

var noop = function() {};
var argv = optimist.argv;
var folder = argv._[0];

if (!folder) {
  optimist.showHelp();
  process.exit(1);
}

var port = process.env.PORT || argv.port;
var dat = new Dat(folder, function(err) {
  if (err) throw err;
  
  dat.listen(port, noop);

  var db = flat.sync(path.join(folder, 'sync.db'));
  var seq = db.get('seq');
  
  if (seq) console.log('last seq', seq);
  
  // haha
  setInterval(function() {
    if (seq) db.put('seq', seq);
  }, 10000);
  
  update();
  
  function update() {
    console.log('creating changes stream...');
    var count = 0;
    
    var reqUrl = 'https://skimdb.npmjs.com/registry/_changes?heartbeat=30000&include_docs=true&feed=continuous' + (seq ? '&since=' + seq : '');
    console.log(reqUrl);
    var changes = request(reqUrl);
    
    changes.pipe(split()).pipe(through.obj({highWaterMark: 20}, function(data, enc, cb) {
      data = JSON.parse(data);
      var doc = data.doc;
      
      changes.on('finish', update);
      changes.on('error', update);
      
      // dat uses .id
      doc.id = doc._id;
      delete doc._id;
      
      // keep the seq around because why not
      doc.couchSeq = seq = data.seq;
      
      dat.get(doc.id, function(err, existing) {
        if (err) return put();
        getAttachments(existing);
      })
      
      function put() {
        dat.put(doc, function(err, latest) {
          if (err) {
            console.error('PUT ERR!', doc, err);
            return cb();
          }
          getAttachments(latest);
        });
      }
      
      function getAttachments(latest) {
        var versions = Object.keys(latest.versions);
      
        // fetch all attachments
        var fns = [];
        versions.map(function(version) {
          var filename = latest.name + '-' + version + '.tgz';
          var tgz = latest.versions[version].dist.tarball;
          if (!tgz) return console.log(latest.name, version, 'has no dist.tarball');
          if (latest.attachments && latest.attachments[filename]) return console.log(filename, 'already in doc');
          
          fns.push(getAttachment);
          
          function getAttachment(cb) {
          
            var ws = dat.createBlobWriteStream(filename, latest, function(err, updated) {
              if (err) return cb(err);
              latest = updated;
              cb();
            })
          
            console.log('tgz GET', tgz);
            request(tgz).pipe(ws);
          }
        })
      
        parallel(fns, function(err, results) {
          if (err) console.error('GET ERR!', err);
          console.log(++count, [latest.id, latest.version]);
          cb();
        })
      };
       
    }));
    
  };
});