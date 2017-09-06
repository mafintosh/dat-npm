var hyperdrive = require('hyperdrive')
var hyperdb = require('hyperdb')
var hyperdiscovery = require('hyperdiscovery')

var init = require('./')
var keys = {
  meta: '0f8a60595af5387d52b053af4a8a4aecd5d6d3799741c3993916798e71ea0730',
  tarballs: '0a8a60595af5387d52b053af4a8a4aecd5d6d3799741c3993916798e71ea0730'
}

init(keys, function (err, datNpm) {
  datNpm.meta.get('/modules/request', function (err, data) {
    console.log(err || JSON.stringify(data[0].value))
    datNpm.tarballs.close()
    datNpm.tarballs.swarm.close()
    datNpm.meta.swarm.close()
  })
})