var hyperdb = require('hyperdb')
var hyperdiscovery = require('hyperdiscovery')

var npmkey = 'f60e9c6ab864afb2e439135b04b41828c20f9f8e4558394a28d2a52cae1ae872'
var db = hyperdb('./npm.db', npmkey, {sparse: true, valueEncoding: 'json'})

db.on('ready', function () {
  var swarm = hyperdiscovery(db, {live: true})
  swarm.once('connection', function () {
    db.get('/modules/aws.js', function (err, data) {
      console.log(err, data)
    })    
  })
})
