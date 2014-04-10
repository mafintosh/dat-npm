#!/usr/bin/env node

var Dat = require('dat');
var mkdirp = require('mkdirp');
var flat = require('flat-file-db');
var read = require('./read-registry-stream');
var through = require('through2');
var path = require('path');
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

mkdirp.sync(folder);

var db = flat.sync(path.join(folder, 'sync.db'));

var dat = new Dat(folder, {serve:false}, function(err) {
	if (err) throw err;

	var update = function() {
		var ws = dat.createWriteStream({
			objects: true,
			primary: ['name']
		});

		ws.on('error', function(err) {
			// ignore for now...
		});

		var seq = db.get('seq');

		read(seq)
			.pipe(through.obj(function(data, _, cb) {
				seq = data.seq;
				delete data.seq;
				cb(null, data);
			}))
			.on('finish', function() {
				db.put('seq', seq, function() {
					setTimeout(update, 5 * 60 * 1000);
				});
			})
			.pipe(ws);

	};

	dat.init(function() {
		update();
		dat.serve({
			port: process.env.PORT || argv.port
		}, function(err, msg) {
			if (err) throw err;
			console.log(msg)
		});
	});
});
