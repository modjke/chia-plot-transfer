
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

  getDrive(size) {
    const drive = this.drives.find(drive => {
      return drive.bytes.available > size
    })

    if (!drive) {
      return null
    }

    return drive.path
  }
}

class Farmer {
  constructor(url) {
    this.url = url
    if (!this.url.endsWith('/')) {
      this.url += '/'
    }

    this.plotCount = 0
  }

  async update() {
    try {
      this.plotCount = (await axios.get(this.url)).data.length
    } catch (ignore) {
      this.plotCount = -1
    }
  }

  async getPlot() {
    try {
      const res = await axios.get(this.url)
      if (!Array.isArray(res.data)) {
        throw new Error(`Recieved data is not an array`)
      }

      if (res.data.length) {
        const [name] = res.data

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
      }
    } catch (error) {
      console.error(error)
    }

    return null;
  }

  async remove({ name }) {
    await axios.get(this.url + 'remove/' + name)
  }
}


void async function main() {
  const driveManager = new DriveManager(config.parentMount)

  console.log(`Updating drives list...`)
  await driveManager.update()
  console.log(driveManager.drives.map(drive => drive.path).join('\n'))

  const farmers = config.farmers.map(url => new Farmer(url))


  void async function fetchPlots() {
    await Promise.all(farmers.map(farmer => farmer.update()))

    for (const farmer of farmers) {
      console.log(`${farmer.url} - ${farmer.plotCount}`)
    }

    farmers.sort((a, b) => b.plotCount - a.plotCount)

    const [ farmer ] = farmers
    if (farmer.plotCount > 0) {
      try {
        const plot = await farmer.getPlot()
        
        console.log(`Found a plot ${farmer.url} > ${plot.name}`)

        await driveManager.update()

        await Promise.all(driveManager.drives.map(({ path }) => chiaExec(`plots add -d ${path}`)))        
        
        const drivePath = await driveManager.getDrive(plot.size)
        if (!drivePath) {
          console.error(`ERROR: NO SPACE LEFT`)
        }
    
        console.log(`Downloading to ${drivePath}`)
        const plotPath = await download(plot, drivePath)

        let retries = 100

        let valid = false
        while (!valid && retries-- > 0) {
          await new Promise(resolve => setTimeout(resolve, 3000)) 

          valid = await chiaValidatePlot(plot.name)          

          if (!valid) {
            console.log(`Plot is not YET valid, may be later...`)            
          }
        }
        
        if (valid) {
          console.log(`Good plot: `, plotPath)
          await farmer.remove(plot)
        } else {
          silentRm(plotPath)
          console.error(`ERROR: INVALID PLOT!!!`, plotPath)
        }
      } catch (error) {
        console.error(error)
      }
    }
  

    setTimeout(fetchPlots, 500)
  }()

}()

function shuffle(array) {
  let n = array.length * 3
  while (n-- > 0) {
    const a = array.length * Math.random() | 0
    const b = array.length * Math.random() | 0
    const t = array[a]
    array[a] = array[b]
    array[b] = t
  }
}

function loadConfig() {
  return JSON.parse(fs.readFileSync(path.resolve(__dirname, 'harvester.json'), 'utf-8'))
}

async function chiaValidatePlot(plot) {
  const { ok, stderr } = await chiaExec(`plots check -g ${plot}`)
  if (!ok) {
    return false
  }

  const match = /Found\s(\d+)\svalid\splots/g.exec(stderr)
  const validPlotCount = parseInt((match || [])[1] || '0')

  if (validPlotCount > 1) {
    console.error(`ERROR: more than one valid plot returned by chiaValidate`)
  }

  return validPlotCount === 1
}

async function chiaExec(command) {
  try {
    // console.log(`[chia] ${command}`)
    const shell = `/bin/bash`
    const { stdout = '', stderr = '' } = await exec(`${shell} -c 'cd ${config.chiaDir}; . ./activate; chia ${command}'`)
    return { ok: true, stdout, stderr }
  } catch (error) {
    console.log(`it's ok if you are on windows`, error)
    return { ok: false, stdout: '', stderr: '' }
  }
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

async function download({ downloadUrl, name, size }, destDir) {
  const dstfpath = path.join(destDir, name)
  silentRm(dstfpath)

  const tmpfname = name + '.tmp'
  const tmpfpath = path.join(destDir, tmpfname)
  silentRm(tmpfpath)
  

  console.log(`Downloading plot to ${tmpfname}...`)
  

  process.stdout.write('Progress: 0%\r')
  const stopWatch = watchFileSize(tmpfpath, 2000, (currentSize) => {
    const prog = (100 * currentSize / size).toFixed(1)
    process.stdout.write(`Progress: ${prog}%\r`)
  })

  try {
    
    await curlExec(`-o ${tmpfpath} ${downloadUrl}`)  
    stopWatch()
   

    console.log(`\nRenaming plot ot ${dstfpath}...`)
    fs.renameSync(tmpfpath, dstfpath)

    return dstfpath
  } catch (error) {
    stopWatch()

    console.error(`Encountered error while downloading plot: ${name}`)
    console.error(error)

    silentRm(tmpfname)
    silentRm(dstfpath)    

    throw new Error(`Unable to download`)
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
