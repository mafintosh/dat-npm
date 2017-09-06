var http = require('http')
var test = require('tape')
var request = require('request')
var rimraf = require('rimraf')
var restApi = require('./rest-api.js')

rimraf.sync('./npm-meta.db')
rimraf.sync('./npm-tarballs.db')

test('get tarball', function (t) {
  var server = restApi(function (err, router, datNpm) {
    var server = http.createServer(function (req, res) {
      console.log(req.method, req.url)
      router(req, res)
    })
    server.listen(8888, function () {
      request({json: true, url: 'http://localhost:8888/cache/request/2.81.0'}, function (err, resp, data) {
        t.equal(resp.statusCode, 200, '200 OK')
        var key = '/tarballs/request-2.81.0.tgz'
        t.deepEqual(data, {key: key, ready: true}, 'json matches')
        datNpm.tarballs.stat(key, function (err, stat) {
          t.ifErr(err, 'no error')
          t.ok(stat, 'stat ok')
          server.close(function () {
            datNpm.tarballs.close()
            datNpm.tarballs.swarm.close()
            datNpm.meta.swarm.close()
            t.end()
          })
        })
      })
    })
  })
})
