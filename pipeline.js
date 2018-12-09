//NOTE: When morph.io executes the scraper, it sets process.env.NODE_ENV
//to 'production'
const getDB = require('rwv-sqlite/lib/db')
const errorTable = require('rwv-sqlite/lib/error')
const InsertStream = require('rwv-sqlite/lib/stream')
const {JSONToString} = require('rwv-sqlite')
const scrape = require('./lib/scrape')

const toStringStream = new JSONToString()

//Setup the database
const pipeline = async ( opts
  /*{
    //An async function that scrapes a page of reports index
    scrapeIndex: async (url) => ({reportsURLs: [urls], pageCount: Interger}),
    //An async function that scrapes a report
    scrapeReport: async (url) => report,
    //Return the URL for the given page number
    urlOfPage: (pageNum) => urlToPage,
    //Return the page number for the given page URL
    pageNumberOfURL: (pageURL) => Integer,
    //The URL to the first page of reports
    startAtPageURL: String,
    //Path to the database config.json
    pathToDbConfig: String
    }*/
) => {
  const {DB} = await getDB( opts.pathToDbConfig, false )
  const insertStream = new InsertStream({}, DB)

  //Parameters passed by env variables have priority
  if( process.env.MORPH_START_PAGE && process.env.MORPH_START_REPORT ){
    Object.assign( opts, {
      startAtReportURI: process.env.MORPH_START_REPORT,
      startAtPageURL: process.env.MORPH_START_PAGE
    })
  }else{
    let error
    if( !process.env.MORPH_CLEAR_ERROR || process.env.MORPH_CLEAR_ERROR != 'clear' ){
      //Check if they are any error from which to start from
      error = await errorTable.get( DB )
    }
    //Consume all errors
    await errorTable.clear( DB )
    if(error){
      Object.assign( opts, {
        startAtReportURI: error.reportURI != "NA" ? error.reportURI : false,
        startAtPageURL: error.pageURL
      })
    }
  }

  const scraperStream = await scrape(DB.db, opts, true)
  return new Promise( (s,f) => {

    //Catch error from the streams
    scraperStream.on('error', f)
    insertStream.on('error', f)
    //done!
    insertStream.on('end', () => s('ok') )

    try{
      //Start scraping and inserting
      scraperStream.pipe(insertStream)
        .pipe(toStringStream)
        .pipe(process.stdout)
    }catch(e){
      f(e)
    }
  }).catch( (e) => {
    const reportURI = e.reportURI || 'NA'
    const pageURL = scraperStream.currentPageURL
    const cause = JSON.stringify( e.message )
    //save th error to the database
    return errorTable.insert( reportURI, cause, pageURL, DB )
      .then( () => {
        throw e
      })
  })
}

module.exports = pipeline
