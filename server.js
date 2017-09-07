var DatNPM = require('./index.js')
DatNPM(function (err, datNpm) {
  console.log('Sharing hypercores', {		
    tarballs: datNpm.tarballs.key.toString('hex'), 		
    meta: datNpm.meta.key.toString('hex')		
  })
  datNpm.startUpdating()
})
