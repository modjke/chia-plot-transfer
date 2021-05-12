
const fs = require('fs')
const { promisify } = require('util')
const path = require('path')
const checkDiskUsage = promisify(require('diskusage').check)
const http = require('http')
const axios = require('axios').default
const stream = require('stream')
const createProgressStream = require('progress-stream')
const exec = promisify(require('child_process').exec)

console.log(`[Harvester] Starting up...`)
const config = loadConfig()

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
        const valid = await chiaValidatePlot(plotPath)
        if (valid) {
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
  const { ok, stderr } = chiaExec(`plots check -g ${plot}`)
  if (!ok) {
    return false
  }

  const match = /Found\s(\d+)\svalid\splots/g.exec(stderr)
  const validPlotCount = (match || [])[1] || 0

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
  return new Promise((resolve, reject) => {
    const dstfname = path.join(destDir, name)

    if (fs.existsSync(dstfname)) {
      return reject(new Error(`dest file already exists...`))
    }

    const tmpfname = path.join(destDir, name + '.tmp')

    console.log(`Downloading plot to ${tmpfname}...`)


    http.get(downloadUrl, response => {
      if (response.statusCode !== 200) {
        reject(new Error(`Non 200 status`))
        return
      }

      const progress = createProgressStream({
        length: size,
        time: 120000
      })

      progress.on('progress', progress => {
        console.log(`Percentage: ${progress.percentage}%, ETA: ${(progress.eta / 60).toFixed(1)} min`)
      })

      stream.pipeline(
        response,
        progress,
        fs.createWriteStream(tmpfname),
        err => {
          if (err) {
            console.error(err)

            silentRm(tmpfname)

            reject(new Error(`Stream ended with error`))
          } else {
            try {
              console.log(`Renaming plot ot ${dstfname}...`)
              fs.renameSync(tmpfname, dstfname)

              resolve(dstfname)
            } catch (error) {
              silentRm(tmpfname)
              silentRm(dstfname)

              reject(new Error(`Unable to rename tmp file into .plot`))
            }
          }
        }
      )
    })
  })

}

function silentRm(filepath) {
  try {
    fs.unlinkSync(filepath)
  } catch (ignore) {

  }
}
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
