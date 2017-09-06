var hyperdrive = require('hyperdrive')
var hyperdb = require('hyperdb')
var hyperdiscovery = require('hyperdiscovery')

var init = require('./')
var keys = {
  meta: '0f8a60595af5387d52b053af4a8a4aecd5d6d3799741c3993916798e71ea0730',
  tarballs: '8c2909a452cc84ac4346bc368052c518b22c9945c5ac5a252082fca1a318fe4a'
}

init(keys, function (err, datNpm) {
  datNpm.tarballs.metadata.once('remote-update', function () {
    datNpm.tarballs.stat('/tarballs/hypercore-6.6.6.tgz', function (err, stat) {
      console.log(err || JSON.stringify(stat))
      datNpm.tarballs.close()
      datNpm.tarballs.swarm.close()
      datNpm.meta.swarm.close()
    })
  })
})
  