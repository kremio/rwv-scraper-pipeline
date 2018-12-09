const path = require('path')
const getDB = require('rwv-sqlite/lib/db')
const reportTable = require('rwv-sqlite/lib/report')
const scrape = require('../../lib/scrape')



//Mocks
const scrapeIndex = jest.fn()
const scrapeReport = jest.fn()
const urlOfPage = (p) => p

const baseOpts = {
  scrapeIndex,
  scrapeReport,
  urlOfPage,
  pageNumberOfURL: (p) =>  p,
  startAtPageURL: 1
}

//Lets us control timers
jest.useFakeTimers()

describe('Scraper stream', () => {

  let db
  const pathToDbConfig = path.resolve(__dirname, '../database.json')


  beforeEach( async (done) =>  {
    setTimeout.mockClear()
    scrapeIndex.mockReset()
    scrapeReport.mockReset()
    
    getDB(pathToDbConfig).then( ({DB, migrations}) =>{
      db = DB
      //Start with a clean database
      migrations.reset( () => migrations.up( done ) )
    })
  })

  afterEach( () => {
    if(db){
      db.close()
      db = undefined
    }
  })

  test( 'No URLs, one page', async (done) => {

    scrapeIndex.mockImplementationOnce(() => ({
      reportURLs: [],
      pageCount: 1
    }))

    const reportStream = await scrape( db.db, baseOpts )
    expect( scrapeIndex ).toHaveBeenCalledTimes(1)

    reportStream.resume()//pipe( process.stdout )
    reportStream.on('end', () => {
      expect( scrapeReport ).not.toHaveBeenCalled()
      done()
    })

  })

  test( 'Wait between requests groups', async (done) => {

    scrapeIndex.mockImplementation( () => ({
      reportsURLs: [1,2,3,4,5],
      pageCount: 1
    }) )

    let c = 0
    const longWait = 999999
    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: longWait
    }, baseOpts) )

    reportStream.on('data', async(chunk) => {
      if( c % 2 == 0 ){
        jest.runOnlyPendingTimers()
      }
    })
    
    reportStream.on('error', (err) => {
      throw err
    })

    reportStream.on('end', () => {
      expect( setTimeout ).toHaveBeenCalledTimes(2)
      expect( setTimeout ).toHaveBeenLastCalledWith(expect.any(Function), longWait)
      expect( scrapeIndex ).toHaveBeenCalledTimes(1)
      expect( scrapeReport ).toHaveBeenCalledTimes(5)
      expect( scrapeReport ).toHaveBeenCalledWith( 1 )
      expect( scrapeReport ).toHaveBeenCalledWith( 2 )
      expect( scrapeReport ).toHaveBeenCalledWith( 3 )
      expect( scrapeReport ).toHaveBeenCalledWith( 4 )
      expect( scrapeReport ).toHaveBeenCalledWith( 5 )
      done()
    })

  })
  
  test( 'Scrape all pages', async (done) => {
    scrapeIndex.mockImplementationOnce(() => ({ //page 1
      reportsURLs: [1],
      pageCount: 3
    })).mockImplementationOnce(() => ({ //page 2
      reportsURLs: [2,3],
    })).mockImplementationOnce(() => ({ //page 3
      reportsURLs: [4,5],
    }))

    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: 1000
    }, baseOpts) )

    let c = 0
    reportStream.on('data', async(chunk) => {
      c++
      switch( c ){
        case 1:
          expect( scrapeReport ).toHaveBeenCalledTimes(1)
          jest.runOnlyPendingTimers()
          break
        case 3:
          expect( scrapeIndex ).toHaveBeenCalledTimes(2)
          jest.runOnlyPendingTimers()
          break
        case 4: expect( scrapeIndex ).toHaveBeenCalledTimes(3)
          break
        default:
      }
    })

    reportStream.on('end', () => {
      expect( scrapeIndex ).toHaveBeenCalledTimes(3)
      expect( scrapeReport ).toHaveBeenCalledTimes(5)
      done()
    })

  })

  test( 'Use correct page URLs', async (done) => {
    scrapeIndex.mockImplementationOnce(() => ({ //page 1
      reportsURLs: [1],
      pageCount: 3
    })).mockImplementationOnce(() => ({ //page 2
      reportsURLs: [2,3],
    })).mockImplementationOnce(() => ({ //page 3
      reportsURLs: [4,5],
    }))

    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: 1
    }, baseOpts) )

    let c = 0
    reportStream.on('data', async(chunk) => {
      ++c
      switch( c ){
        case 1:
          jest.runOnlyPendingTimers()
          break
        case 3:
          jest.runOnlyPendingTimers()
          break
        default:
      }
    })

    reportStream.on('end', () => {
      expect( scrapeIndex ).toHaveBeenCalledTimes(3)
      expect( scrapeIndex ).toHaveBeenCalledWith( baseOpts.startAtPageURL )
      expect( scrapeIndex ).toHaveBeenCalledWith( urlOfPage(2) )
      expect( scrapeIndex ).toHaveBeenCalledWith( urlOfPage(3) )
      done()
    })

  })

  test( 'Start scraping at given report URI (included) and given page URL', async (done) => {
    const startAtPageURL = urlOfPage(2)
    const startAtReportURI = 2
    scrapeIndex.mockImplementationOnce( () => ({ //page 1
      reportsURLs: [1,2,3],
      pageCount: 3
      })
    ).mockImplementationOnce( () => ({ //page 2
      reportsURLs: [4],
      })
    )

    const reportStream = await scrape( db.db, Object.assign({
       groupSize: 2, groupInterval: 1, stopAtReportURI: 1
    }, baseOpts, { startAtPageURL, startAtReportURI }) )

    let c = 0
    reportStream.on('data', async(chunk) => {
      switch( c ){
        case 1:
          throw new Error("Should not have scraped this report.")
          break
        default:
          jest.runOnlyPendingTimers()
      }
    })

    reportStream.on('end', () => {
      expect( scrapeIndex ).toHaveBeenCalledTimes(2)
      expect( scrapeReport ).toHaveBeenCalledTimes(3)
      expect( scrapeReport ).toHaveBeenCalledWith( 2 )
      expect( scrapeReport ).toHaveBeenCalledWith( 3 )
      expect( scrapeReport ).toHaveBeenCalledWith( 4 )
      done()
    })

  })

  test( 'No scraping if start report URI not found', async (done) => {
    const startAtPageURL = urlOfPage(2)
    const startAtReportURI = 5
    scrapeIndex.mockImplementationOnce(() => ({ //page 2
      reportsURLs: [1,2,3],
      pageCount: 3
    })).mockImplementationOnce(() => ({ //page 3
      reportsURLs: [4],
    }))

    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: 1, stopAtReportURI: 1
    }, baseOpts, {startAtPageURL, startAtReportURI}) )

    reportStream.on('data', async(chunk) => {
      throw new Error("Should not have scraped this report.")
    })

    reportStream.on('end', () => {
      expect( scrapeIndex ).toHaveBeenCalledTimes(2)
      expect( scrapeReport ).not.toHaveBeenCalled()
      done()
    })

  })
  
  test( 'Stop scraping at given report URI (excluded)', async (done) => {
    scrapeIndex.mockImplementationOnce(() => ({ //page 1
      reportsURLs: [1],
      pageCount: 3
    })).mockImplementationOnce(() => ({ //page 2
      reportsURLs: [2,3],
    })).mockImplementationOnce(() => ({ //page 3
      reportsURLs: [4,5],
    }))

    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: 1, stopAtReportURI: 3
    }, baseOpts) )


    let c = 0
    reportStream.on('data', async(chunk) => {
      expect( scrapeReport ).toHaveBeenCalledTimes(++c)

      switch( c ){
        case 1:
          jest.runOnlyPendingTimers()
          break
        case 3:
          throw new Error("Went beyond the given report.")
          break
        default:
      }
    })

    reportStream.on('end', () => {
      expect( scrapeIndex ).toHaveBeenCalledTimes(2)
      expect( scrapeReport ).toHaveBeenCalledTimes(2)
      expect( scrapeReport ).toHaveBeenCalledWith( 1 )
      expect( scrapeReport ).toHaveBeenCalledWith( 2 )
      done()
    })

  })

  test( 'Only scrape reports between start URI and end URI (excluded)', async(done) => {

    scrapeIndex.mockImplementationOnce(() => ({ //page 1
      reportsURLs: [1,2],
      pageCount: 3
    })).mockImplementationOnce(() => ({ //page 2
      reportsURLs: [3,4],
    })).mockImplementationOnce(() => ({ //page 3
      reportsURLs: [5,6],
    }))

    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: 1, startAtReportURI: 3, stopAtReportURI: 6
    }, baseOpts) )


    let c = 0
    reportStream.on('data', async(chunk) => {
      switch( c ){
        case 1:
        case 2:
        case 6:
          throw new Error("Should not have scraped this report.")
        default:
         jest.runOnlyPendingTimers()
      }
    })

    reportStream.on('end', () => {
      expect( scrapeIndex ).toHaveBeenCalledTimes(3)
      expect( scrapeReport ).toHaveBeenCalledTimes(3)
      expect( scrapeReport ).toHaveBeenCalledWith( 3 )
      expect( scrapeReport ).toHaveBeenCalledWith( 4 )
      expect( scrapeReport ).toHaveBeenCalledWith( 5 )
      done()
    })


  })

  test( 'Outputs the reports', async(done) => {
    scrapeIndex.mockImplementationOnce(() => ({ //page 1
      reportsURLs: [1,2],
      pageCount: 2
    })).mockImplementationOnce(() => ({ //page 2
      reportsURLs: [3],
    }))

    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: 1
    }, baseOpts) )
    
    let c = 1
    scrapeReport.mockImplementation((d) => d)
    const chunks = []
    reportStream.on('data', async(chunk) => {
      expect( chunk ).toEqual(c)
      chunks.push( chunk )
      c++
      switch( chunk ){
        case 2:
          jest.runOnlyPendingTimers()
          break
          default:
      }
    })

    reportStream.on('end', () => {
      expect( chunks ).toEqual( [1,2,3] )
      done()
    })

  })

  test( 'Nothing to scrape', async (done) => {
    scrapeIndex.mockImplementationOnce(() => ({ //page 1
      reportsURLs: [1],
      pageCount: 3
    }))

    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: 1, stopAtReportURI: 1
    }, baseOpts) )


    reportStream.on('data', (chunk) => {
      done.fail( new Error('No chunk should have been emitted by the stream') )
    })

    reportStream.on('end', () => {
      expect( scrapeIndex ).toHaveBeenCalledTimes(1)
      expect( scrapeReport ).not.toHaveBeenCalled()
      done()
    })

  })

  test( 'Throws parsing error and ends stream', async (done) => {

    scrapeIndex.mockImplementationOnce(() => ({ //page 1
      reportsURLs: [1],
      pageCount: 3
    })).mockImplementationOnce(() => ({ //page 2
      reportsURLs: [2,3],
    })).mockImplementationOnce(() => ({ //page 3
      reportsURLs: [4,5],
    }))


    scrapeReport.mockImplementation((url) => {
      if(url == 3){
        throw new Error('Parsing failed')
      }
    })

    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: 1
    }, baseOpts) )


    reportStream.on('error', (err) => {
      expect( err.message ).toEqual('Parsing failed')
    })


    reportStream.on('end', () => {
      expect( scrapeIndex ).toHaveBeenCalledTimes(2)
      expect( scrapeReport ).toHaveBeenCalledTimes(3)
      done()
    })

    reportStream.on('data', (chunk) => {
      jest.runOnlyPendingTimers()
    })


  })

  test( 'Stop parsing after already scraped limit reached', async (done) => {
    scrapeIndex.mockImplementationOnce(() => ({ //page 1
      reportsURLs: ['http://1','http://2'],
      pageCount: 3
    })).mockImplementationOnce(() => ({ //page 2
      reportsURLs: ['http://3','http://4'],
    })).mockImplementationOnce(() => ({ //page 3
      reportsURLs: ['http://5','http://6'],
    }))

    //Add reports to the DB
    const baseReport = {
      description:'description',
      startDate: new Date().toISOString(),
      iso3166_2: 'DE-BE',
      locations: [{
        subdivisions:['somewhere','over the rainbow'],
      }],
      sources: [{
        name:'source'
      }]
    }
    await reportTable( {...baseReport, uri:'http://1' }, db )
    await reportTable( {...baseReport, uri:'http://2' }, db )
    await reportTable( {...baseReport, uri:'http://4' }, db )


    const reportStream = await scrape( db.db, Object.assign({
      groupSize: 2, groupInterval: 1, alreadyScrapedLimit: 3
    }, baseOpts) )


    let c = 0
    reportStream.on('data', async(chunk) => {
      switch( c ){
        case 'http://1':
        case 'http://2':
        case 'http://4':
        case 'http://5':
        case 'http://6':
          throw new Error("Should not have scraped this report.")
        default:
         jest.runOnlyPendingTimers()
      }
    })

    reportStream.on('end', () => {
      expect( scrapeIndex ).toHaveBeenCalledTimes(2)
      expect( scrapeReport ).toHaveBeenCalledTimes(1)
      expect( scrapeReport ).toHaveBeenCalledWith( 'http://3' )
      done()
    })

  })

})
