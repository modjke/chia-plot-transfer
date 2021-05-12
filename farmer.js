const { promises: fs, constants, createReadStream } = require('fs')
const path = require('path')

void async function main() {
  const config = JSON.parse(
    await fs.readFile(path.join(__dirname, 'farmer.json'), 'utf-8')
  )

  const port = config.port || 9999
  const hostname = config.hostname || 'localhost'
  const plotsDir = path.resolve(config.plotsDir)  

  try {
    await fs.access(plotsDir, constants.W_OK | constants.R_OK)
    const stats = await fs.stat(plotsDir)
    
    if (!stats.isDirectory()) {
      throw new Error(`Not a directory`)
    }    
  } catch (error) {
    console.error(error)    
    
    return
  }

  startServer(plotsDir, port, hostname)
}()


function startServer(plotsDirectory, port = 9999, hostname = '0.0.0.0') {
  const Koa = require('koa');
  const Router = require('@koa/router');
  const glob = require('glob')
  const koaRespond = require('koa-respond');
  
  const app = new Koa();
  const router = new Router();

  router.get('/', async (ctx, next) => {    
    ctx.body = getPlots()
  });

  router.get('/download/:name', async (ctx, next) => {

    try {
      const { name } = ctx.params

      
      if (getPlots().indexOf(name) === -1) {
        ctx.notFound()
        return
      }

      const file = path.resolve(plotsDirectory, name) 
      
      const stats = await fs.stat(file)

      ctx.body = createReadStream(file)

      ctx.set('Content-disposition', 'attachment; filename=' + name);
      ctx.set('Content-type', 'application/octet-stream');
      ctx.set('Content-length', stats.size)
    } catch (error) {
      console.log(error)

      ctx.internalServerError()
      return
    }
  })

  router.get('/remove/:name', async (ctx, next) => {
    try {
      const { name } = ctx.params
      
      if (getPlots().indexOf(name) === -1) {
        ctx.notFound()
        return
      }

      const file = path.resolve(plotsDirectory, name) 

      await fs.unlink(file)

      ctx.body = { removed: true }
    } catch (error) {
      console.log(error)

      ctx.internalServerError()
    }

  })

  app
    .use(koaRespond())
    .use(router.routes())
    .use(router.allowedMethods());

  app.listen(port, hostname, () => {
    console.log(`Listening at http://${hostname}:${port}`)
  })

  function getPlots() {
    return glob.sync('*.plot', { cwd: plotsDirectory })
  }
}
