var http = require('http')
var Router = require("routes-router")
var request = require('request')
var concat = require('concat-stream')

module.exports = function(remote, port) {
  var server = http.createServer(handler)

  function handler(req, res) {
    var router = Router()
    console.log('<', req.url)
    router.addRoute("/:module", module)
    router(req, res)
  
    function module(req, res, opts) {
      var module = opts.module
      var uri = remote + '/api/rows/' + module
      console.log('>', uri)
      
      var proxyReq = request(uri)
      
      proxyReq.on('error', function(err) {
        console.error('>', uri, err.message)
        res.statusCode = 404
        res.end(JSON.stringify({error: 'not found'}))
      })
      
      var rewriteUrls = concat(function(data) {
        var doc = JSON.parse(data)
        Object.keys(doc.versions).map(function(version) {
          var orig = doc.versions[version].dist.tarball
          var filename = orig.split('/')
          filename = filename[filename.length - 1]
          doc.versions[version].dist.tarball = uri + '/' + filename
        })
        res.end(JSON.stringify(doc))
      })
      
      proxyReq.pipe(rewriteUrls)
    }
  }

  return server
}
