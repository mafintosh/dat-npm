var http = require('http')
var Router = require("routes-router")
var request = require('request')

var server = http.createServer(handler)
var dat = 'http://localhost:6461'

function handler(req, res) {
  var router = Router()
  console.log(req.url)
  router.addRoute("/:module", module)
  router(req, res)
  
  function module(req, res, opts) {
    var module = opts.module
    request(dat + '/api/' + module).pipe(res)
  }
}

server.listen(8080)
