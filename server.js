var http = require('http')
var server = require('./rest-api.js')(function (err, router, datNpm) {
  var server = http.createServer(function (req, res) {
    console.log(req.method, req.url)
    router(req, res)
  })
  var port = process.env.PORT || 8080
  server.listen(port, function () {
    console.log('listening on', port)
    console.log('Sharing hypercores', {
      tarballs: datNpm.tarballs.key.toString('hex'), 
      meta: datNpm.meta.key.toString('hex')
    })
    datNpm.startUpdating()
  })
})
