var hyperdb = require('hyperdb')
var hyperdiscovery = require('hyperdiscovery')

var npmkey = '0f8a60595af5387d52b053af4a8a4aecd5d6d3799741c3993916798e71ea0730'
var db = hyperdb('./npm.db', npmkey, {sparse: true, valueEncoding: 'json'})

db.on('ready', function () {
  var swarm = hyperdiscovery(db, {live: true})
  swarm.once('connection', function () {
    db.get('/modules/aws.js', function (err, data) {
      console.log(err, data)
    })    
  })
})
