
const fs = require('fs')
const { promisify } = require('util')
const path = require('path')
const checkDiskUsage = promisify(require('diskusage').check)
const axios = require('axios').default
const Downloader = require('nodejs-file-downloader');

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
    })


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

    this.busy = false
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
    await driveManager.update()

    for (const { path } of driveManager.drives) {
      await chiaExec(`plots add -d ${path}`)
    }

    for (const farmer of farmers) {
      if (farmer.busy) continue

      try {
        const plot = await farmer.getPlot()

        if (!plot) continue

        console.log(`Found a plot ${farmer.url} > ${plot.name}`)

        const drivePath = await driveManager.getDrive(plot.size)
        if (!drivePath) {
          console.error(`ERROR: NO SPACE LEFT`)
        }

        const plotPath = await download(plot, drivePath)

        let retries = 10

        let valid = false
        while (!valid && retries-- > 0) {
          await new Promise(resolve => setTimeout(resolve, 5000)) 

          valid = await chiaValidatePlot(plotPath)          

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


      setTimeout(fetchPlots, 60000)
    }
  }()

}()

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
    console.log(`[chia] ${command}`)
    const shell = `/bin/bash`
    const { stdout = '', stderr = '' } = await exec(`${shell} -c 'cd ${config.chiaDir}; . ./activate; chia ${command}'`)
    return { ok: true, stdout, stderr }
  } catch (error) {
    console.log(`it's ok if you are on windows`, error)
    return { ok: false, stdout: '', stderr: '' }
  }
}

async function download({ downloadUrl, name, size }, destDir) {

  
  const dstfpath = path.join(destDir, name)

  if (fs.existsSync(dstfpath)) {
    throw new Error(`dest file already exists...`)
  }

  const tmpfname = name + '.tmp'
  const tmpfpath = path.join(destDir, tmpfname)
  silentRm(tmpfpath)
  

  console.log(`Downloading plot to ${tmpfname}...`)
  
  process.stdout.write('Progress: 0%\r')
  const downloader = new Downloader({     
    url: downloadUrl,     
    directory: destDir,
    fileName: tmpfname,
    onProgress: function(percentage,chunk,remainingSize) {
      process.stdout.write('Progress: ' + percentage + '\r')
    } 
  }) 


  try {
    await downloader.download();   

    console.log(`\nRenaming plot ot ${dstfpath}...`)
    fs.renameSync(tmpfpath, dstfpath)

    return dstfpath
  } catch (error) {
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