var Router = require("routes-router")
var request = require('request')
var concat = require('concat-stream')
var semver = require('semver')
var pump = require('pump')
var init = require('./index.js')
var validateName = require('validate-npm-package-name')

module.exports = function (cb) {
  init(function (err, datNpm) {
    if (err) return cb(err)
    var router = Router()
    router.addRoute("/", function (req, res, opts) {
      res.writeHead("200")
      res.end(JSON.stringify({'version': '1.0.0'}))
    })
    router.addRoute("/cache/:module/:version", module)
    cb(null, router, datNpm)
    
    function module (req, res, opts) {
      var module = opts.params.module
      var version = opts.params.version
      var valid = validateName(module)
      if (!valid.validForOldPackages) {
        res.writeHead(400)
        return res.end(JSON.stringify({error: 'invalid package name'}))
      }
      if (!version || !semver.valid(version)) {
        res.writeHead(400)
        return res.end(JSON.stringify({error: 'invalid package version'}))
      }
      var tarball = `${module}-${version}.tgz`
      var key = '/tarballs/' + tarball
      var tarballUrl = `https://registry.npmjs.org/${module}/-/${tarball}`
      datNpm.tarballs.stat(key, function (err, stat) {
        if (err) return fetch()
        res.writeHead(200)
        return res.end(JSON.stringify({ready: true, key: key}))
      })
    
      function fetch () {
        request.head(tarballUrl, function (err, resp) {
          if (resp.statusCode !== 200) {
            res.writeHead(404)
            return res.end(JSON.stringify({error: 'module version not found'}))
          }
          pump(request(tarballUrl), datNpm.tarballs.createWriteStream(key), function (err) {
            if (err) {
              res.writeHead(500)
              return res.end(JSON.stringify({error: err.message}))
            }
            res.writeHead(200)
            res.end(JSON.stringify({ready: true, key: key}))
          })
        })
      }
    }
  })
}
