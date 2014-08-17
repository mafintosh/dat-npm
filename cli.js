#!/usr/bin/env node

var createProxy = require('./proxy.js')

var remote = process.argv[2] || 'http://localhost:6461'
var port = 9991

var proxy = createProxy(remote, port)

proxy.listen(port, function(err) {
  if (err) throw err
  console.log('--registry=http://localhost:9991')
})
