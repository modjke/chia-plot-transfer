
const fs = require('fs')
const { promisify } = require('util')
const path = require('path')
const checkDiskUsage = promisify(require('diskusage').check)
const axios = require('axios').default
const exec = promisify(require('child_process').exec)

console.log(`[Harvester] Starting up...`)
const config = loadConfig()


class DriveManager {
  constructor(parentMount) {
    this.parentMount = path.resolve(__dirname, parentMount)
    this.reservations = {}
  }

  async update() {
    const entries = fs.readdirSync(this.parentMount).map(entry => path.join(this.parentMount, entry))
    const drivePaths = []
    const dirContentStats = entries.map(entry => fs.statSync(entry))

    for (let i = 0; i < dirContentStats.length; i++) {
      const stats = dirContentStats[i]
      const entry = entries[i]
      if (stats.isDirectory()) {
        drivePaths.push(entry)
      }
    }

    const drivesFreeSpaces = await Promise.all(drivePaths.map(p => checkDiskUsage(p)))


    this.drives = drivePaths.map((p, i) => {
      const bytes = drivesFreeSpaces[i]

      return {
        path: p,
        bytes
      }
    }).filter(d => {
      if (config.ignore) {
        return config.ignore.indexOf(d.path) === -1
      }
      return true
    });
  }

  makeReservation(size) {
    const drive = this.drives.find(drive => {
      return !this.reservations[drive.path] && drive.bytes.available > size
    })

    if (!drive) {
      return null
    }

    return this.reservations[drive.path] = {
      dir: drive.path,
      release: () => {
        delete this.reservations[drive.path]
      }
    }
  }
}

class Farmer {
  constructor(url) {
    this.url = url
    if (!this.url.endsWith('/')) {
      this.url += '/'
    }
    
    this.plots = []
    this.error = false
    this.busy = false
  }

  get plotCount() {
    return this.error ? -1 : this.plots.length
  }

  async update() {
    try {
      const res = await axios.get(this.url)
      if (!Array.isArray(res.data)) {
        throw new Error(`Recieved data is not an array`)
      }

      this.plots = await Promise.all(res.data.map(async name => {
        const downloadUrl = this.url + 'download/' + name

        const { headers: { 'content-length': contentLength } } = await axios.head(downloadUrl)
        const size = parseInt(contentLength)
        if (isNaN(size)) {
          throw new Error(`Invalid content-length`, contentLength)
        }

        return {
          downloadUrl,
          size,
          name
        }
      }))

      this.error = false     
    } catch (ignore) {
      this.error = true
    }
  }

  async download(driveManager) {
    if (this.busy) {
      console.log(this.url, `Trying to download while busy`)
      return
    }

    if (this.error) {
      console.error(this.url, `Not downloading anything due to error`)
      return
    }

    const [ plot ] = this.plots

    if (!plot) {
      console.error(this.url, `Not downloading anything due to no plots`)
      return
    }

    
    const reservation = driveManager.makeReservation(plot.size)
    if (!reservation) {
      console.error(`Unable to make reservation :(`)
      return
    }

    this.busy = true
    this.plot = plot
    this.percent = 0

    const dstfile = path.join(reservation.dir, plot.name)        
    const tmpfile = path.join(reservation.dir, plot.name + '.tmp')
    
    const stopWatch = watchFileSize(tmpfile, 2000, size => {
      this.percent = 100 * size / plot.size      
    })

    try {      
      await __download(plot.downloadUrl, plot.size, tmpfile, dstfile)
      await this.remove(plot)
      this.percent = 100
    } catch (error) {
      console.error(this.url, `Encountered error while downloading plot: ${plot.name}`)
      console.error(error)
    }

    reservation.release()
    stopWatch()

    this.busy = false
  }

  async remove({ name }) {
    await axios.get(this.url + 'remove/' + name)
  }
}


void async function main() {
  const driveManager = new DriveManager(config.parentMount)

  console.log(`Updating drives list...`)
  await driveManager.update()
  // console.log(driveManager.drives.map(drive => drive.path).join('\n'))

  const farmers = config.farmers.map(url => new Farmer(url))

  async function fetchNextPlot() {    
    await Promise.all(farmers.map(farmer => farmer.update()))

    console.log(new Date())      
    for (const farmer of farmers) {      
      console.log(`${farmer.url} - ${farmer.plotCount}`)
      if (farmer.busy) {
        console.log(`- (${farmer.percent.toFixed(1)}%) -> ${farmer.plot.name}`)
      }
    }

    const sortedFarmersThatAreNotBusy = farmers
      .filter(f => !f.busy && f.plotCount > 0)
      .sort((a, b) => b.plotCount - a.plotCount)

    if (sortedFarmersThatAreNotBusy.length === 0) {
      return
    }

    const [ farmer ] = sortedFarmersThatAreNotBusy
    
    await driveManager.update()
    
    farmer.download(driveManager)
  }

   void async function tick() {
    await fetchNextPlot()
    setTimeout(tick, 30000)
  }()
}()


function loadConfig() {
  return JSON.parse(fs.readFileSync(path.resolve(__dirname, 'harvester.json'), 'utf-8'))
}


async function curlExec(command) {
  try {
    // console.log(`[curl] ${command}`)
    const shell = `/bin/bash`
    const { stdout = '', stderr = '' } = await exec(`${shell} -c 'curl ${command}'`)
    return { ok: true, stdout, stderr }
  } catch (error) {
    console.log(`it's ok if you are on windows`, error)
    return { ok: false, stdout: '', stderr: '' }
  }
}

async function __download(downloadUrl, size, tmpfile, dstfile) {  
  silentRm(dstfile)
  silentRm(tmpfile)
  
  try {
    await curlExec(`-o ${tmpfile} ${downloadUrl}`)  

    // size check
    const stat = fs.statSync(tmpfile)
    if (stat.size !== size) {
      throw new Error(`Size mismatch: ${stat.size}, but expected: ${size}`)  
    }
   
    console.log(`\nRenaming plot ot ${dstfile}...`)
    fs.renameSync(tmpfile, dstfile)    
  } catch (error) {        
    silentRm(tmpfile)
    silentRm(dstfile)    

    throw error
  } 
}

function silentRm(filepath) {
  try {
    fs.unlinkSync(filepath)
  } catch (ignore) {

  }
}

function watchFileSize(fpath, ms, callback) {
  const interval = setInterval(function () {
    try {
      callback(fs.statSync(fpath).size)
    } catch (error) {}
    
  }, ms)

  return function () {
    clearInterval(interval)
  }
}
