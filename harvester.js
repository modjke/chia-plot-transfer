
const { promises: fs, constants, stat } = require('fs')
const { promisify } = require('util')
const path = require('path')
const checkDiskUsage = promisify(require('diskusage').check)
const http = require('http')
const axios = require('axios').default

void async function main() {
  console.log(`[Harvester] Starting up...`)

  const config = await loadConfig()
  const driveManager = new DriveManager(config.parentMount)

  console.log(`Updating drives list...`)
  await driveManager.update()
  console.log(driveManager.drives.map(drive => drive.path).join('\n'))

  const farmers = config.farmers.map(url => new Farmer(url))

  
  void async function fetchPlots() {
    console.log(`Updating drives list...`)
    await driveManager.update()

    try {
      for (const farmer of farmers) {        
        if (farmer.busy) continue

        console.log(`Checking plots at ${farmer.url}...`)
        const plot = await farmer.getPlot()

        if (!plot) continue

        const drivePath = await driveManager.getDrive(plot.size)
        if (!drivePath) {
          console.error(`NO SPACE LEFT`)
        }
        const plotPath = await downloadPlot(farmer, plot, drivePath)
        const valid = await validatePlot(plotPath)
      }
    } catch (error) {
      console.log(error)
    }

    // setTimeout(fetchPlots, 60000)
  }()
}()


async function loadConfig() {
  return JSON.parse(await fs.readFile(path.resolve(__dirname, 'harvester.json'), 'utf-8'))
}
class DriveManager {
  constructor(parentMount) {
    this.parentMount = path.resolve(__dirname, parentMount)    
  }

  async update() {
    const entries = (await fs.readdir(this.parentMount)).map(entry => path.join(this.parentMount, entry))
    const drivePaths = []
    const dirContentStats = await Promise.all(entries.map(entry => fs.stat(entry)))
    
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
      return drive.bytes.available <= size
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
        
        const { headers: { 'content-length': contentLength } }= await axios.head(downloadUrl)
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

}
 