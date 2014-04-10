var request = require('request');
var JSONStream = require('JSONStream');
var through = require('through2');

var parseRepository = function(repo) {
	if (!repo) return undefined;
	if (typeof repo === 'object') return parseRepository(repo.url);
	if (typeof repo !== 'string') return undefined;
	if (repo.split('/').length === 2) return 'git://github.com/'+repo+'.git';
	return repo;
};

var parseMaintainer = function(maintainers) {
	if (!Array.isArray(maintainers)) return undefined;
	var m = maintainers[0];
	if (m.name && m.email) return m.name+' <'+m.email+'>';
	return undefined;
};

var map = function(npm, seq) {
	if (!npm.time) return;

	var versions = Object.keys(npm.time);
	if (!versions.length || !npm.versions) return;

	var min = versions.reduce(function(min, cur) {
		if (!npm.versions[cur]) return min;
		if (!npm.versions[min]) return cur;
		return npm.time[min] < npm.time[cur] ? min : cur;
	});

	var max = versions.reduce(function(max, cur) {
		if (!npm.versions[cur]) return max;
		if (!npm.versions[max]) return cur;
		return npm.time[max] > npm.time[cur] ? max : cur;
	});

	var latest = npm.versions[max];
	if (!latest) return;

	return {
		seq: seq,
		name: latest.name,
		version: latest.version,
		tarball: latest.dist.tarball,
		description: latest.description || '',
		modified: npm.time.modified || npm.time[max],
		created: npm.time.created || npm.time[min],
		maintainer: parseMaintainer(npm.maintainers || latest.maintainers),
		repository: parseRepository(latest.repository),
		dependencies: Object.keys(latest.dependencies || {}).length,
		devDependencies: Object.keys(latest.devDependencies || {}).length
	};
};

var read = function(partial) {
	var qs = {include_docs:true};
	if (partial) qs.since = partial;

	return request('http://isaacs.iriscouch.com/registry/_changes', {qs:qs})
		.pipe(JSONStream.parse('results.*'))
		.pipe(through.obj(function(data, enc, cb) {
			cb(null, map(data.doc, data.seq));
		}));
};

module.exports = read;