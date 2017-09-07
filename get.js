var hyperdrive = require('hyperdrive')
var hyperdb = require('hyperdb')
var hyperdiscovery = require('hyperdiscovery')

var DatNPM = require('./')
var keys = {
  meta: 'b87bbf4afad0ecb9c12ae6e14605251c938074de5dbda76ab45f740d90283dfa',
  tarballs: 'c1cd8b35e142fd25055e53a0c18ece5f91d4845c899faa8c3d1d297adf0ba68e'
}

var module = process.argv[2] || 'pushpop'

DatNPM(keys, function (err, datNpm) {
  datNpm.meta.once('remote-update', function () {
    datNpm.meta.get('/modules/' + module, function (err, data) {
      if (err) return console.log(err)
      var meta = data[0].value
      console.log("metadata", JSON.stringify(meta))
      var latest = module + '-' + meta['dist-tags'].latest + '.tgz'
      datNpm.tarballs.stat(DatNPM.hashFilename(latest), function (err, stat) {
        if (err) throw err
        console.log("stat", JSON.stringify(stat))
        datNpm.tarballs.close()
        datNpm.tarballs.swarm.close()
        datNpm.meta.swarm.close()        
      })
    })    
  })
})