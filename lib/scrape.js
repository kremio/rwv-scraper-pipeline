const { promisify } = require('util')
const { Readable } = require('stream')
/*
 * 1: Parse the default index page at https://muenchen-chronik.de/chronik/
 * 2a: If there is a given 'untilReportURI', scrape the reports from the default page until untilReportURI is reached
 * 2b: otherwise, scrape all reports until the last page
 * 
 * A parameter configures how many HTTP requests can be made at once, 5
 * should be a good default
 * A parameter configures how long to wait between a group of requests
 */

/*
 * Need a queue
 * 
 * 
 */
class ReportStream extends Readable{
  constructor(queue, urlOfPage){
    //Call the Readable Stream constructor
    super({
      objectMode: true
    })
    this.urlOfPage = urlOfPage
    this.queue = queue
    this.queue.onData = (data) => {
      return this.push(data)
    }
    this.queue.onEnd = (data) => {
      if(data){
        this.push(data)
      }
      this.push(null)
    }
    this.queue.onError = (error) => {
      this.emit('error', error)
      this.push(null)
    }
  }

  /*
   * Useful to locate where a report makes the scraper choke.
   */
  get currentPageURL(){
    return this.urlOfPage( this.queue.currentPage )
  }

  /*
   * Implementation of Readable
   * see: https://nodejs.org/api/stream.html#stream_implementing_a_readable_stream
   */
  _read(){
    this.queue.readStart()
  }


}


class RequestsQueue  {
  constructor( db, scrapeIndex, scrapeReport, urlOfPage, pageCount, reportsURLs = [], groupSize, groupInterval, currentPage, startAtReportURI = false, stopAtReportURI, alreadyScrapedLimit = 10 ){
    this.db = db
    this.scrapeIndex = scrapeIndex
    this.scrapeReport = scrapeReport
    this.urlOfPage = urlOfPage
    this.pageCount = pageCount
    this.currentPage = currentPage
    this.initialReportsURLs = reportsURLs
    this._reportsURLs = []
    this.currentReportURL = ''

    this.groupSize = groupSize
    this.groupInterval = groupInterval
    this.requestsGroup = []

    this.startAtReportURI = startAtReportURI
    this.stopAtReportURI = stopAtReportURI
    this.alreadyScrapedLimit = alreadyScrapedLimit
    this.howManyAlreadyScraped = 0
    this.timeout
    this.started = false
    this.done = false

    this.onData = () => true
    this.onEnd = () => {}
    this.onError = (error) => { console.error(error) }


  }

  get reportsURLs(){
    return this._reportsURLs
  }

  set reportsURLs( urls ){
    if( !this.startAtReportURI ){
      this._reportsURLs = urls
      return
    }
    //If the scraper was configured to start at a specific report
    //we need to filter out the other reports
    const findIndex = urls.findIndex( (url) => url == this.startAtReportURI )
    if( findIndex < 0 ){ //could not find the url, it could be in next page
      this._reportsURLs = []
      return
    }
    this.startAtReportURI = false //so we don't waste time filtering again
    this._reportsURLs = urls.slice( findIndex )
  }

  isDone(){
    return this.done || (this.reportsURLs.length == 0 && this.currentPage == this.pageCount)
  }

  readStart(){
    if(!this.started){
      this.started = true
      //Sets the initial reportURLs
      this.reportsURLs = this.initialReportsURLs
      this.filterAlreadyScraped().then( () =>  this.next() )
    }
  }

  /*
   * Check if the database already contains some of the reports in reportsURLs,
   * if any found, remove them from reportsURLs and add there count to
   * howManyAlreadyScraped
   */
  async filterAlreadyScraped(){
    //Get the existing reports' URIs
    const countStatement = this.db.prepare( `SELECT uri FROM data WHERE uri IN (${this.reportsURLs.map(() => '?').join(',')})`, this.reportsURLs )
    const asyncAll = promisify(countStatement.all).bind(countStatement)
    const alreadyInDb = (await asyncAll()).map((row) => row.uri)
    countStatement.finalize()

    //Filter out the existing reports
    this.reportsURLs = this.reportsURLs.filter((uri) => !alreadyInDb.includes( uri ))
    //Update the count
    this.howManyAlreadyScraped += alreadyInDb.length
  }

  async next(){
    try{
      if( this.timeout ){
        return //wait for the interval between calls to pass
      }

      this.timeout = undefined

      if( this.isDone() ){
        this.onEnd() //done
        return
      }
      if( this.reportsURLs.length == 0 ){ //all the reports of the page have been processed
        if(this.howManyAlreadyScraped >= this.alreadyScrapedLimit){
          //looks like there is nothing new, let's call it a day
          this.onEnd() //done
          return
        }
        //Load next page
        this.currentPage++
        const {reportsURLs} = await this.scrapeIndex( this.urlOfPage( this.currentPage ) )
        this.reportsURLs = reportsURLs
        await this.filterAlreadyScraped()
        return this.next()
      }

      //Get next batch of requests and remove them from the queue
      let nextBatch = this.reportsURLs
        .splice(0, this.groupSize )

      if( this.stopAtReportURI && nextBatch.includes( this.stopAtReportURI ) ){
        //Only process the reports up until the given URI and we are done!
        const until = nextBatch.indexOf( this.stopAtReportURI )
        nextBatch = nextBatch.splice(0, until)
        this.done = true
      }

      await Promise.all( nextBatch.map( (url) => this.scrapeReport(url) ) )
        .then( async (reports) => {
          if( this.isDone() ){
            clearTimeout( this.timeout )
            this.timeout = undefined
            const last = reports.pop()
            reports.every( (report) => this.onData(report) )

            this.onEnd( last ) //done
            return
          }

          //Keep processing the queue after a pause
          this.timeout = setTimeout( () => {
            this.timeout = undefined
            this.next()
          }, this.groupInterval ) //wait a moment before continuing

          const keepGoing = reports.every( (report) => this.onData(report) )

          if( !keepGoing ){
            clearTimeout( this.timeout )
            this.timeout = undefined
            this.onEnd()
            return
          }
        })
    }catch(e){
      this.onError(e)
    }
  }
}

const scrape = async (db, options, verbose = false) => {
  const {scrapeIndex, scrapeReport, urlOfPage} = options
  //Override defaults with given options
  const opts = Object.assign({
    groupSize: 5,
    groupInterval: 30000, //in ms
    stopAtReportURI: false,
    startAtReportURI: false,
    alreadyScrapedLimit: 10
  }, options)

  if(verbose){
    console.log("## Scraper setup:", opts)
  }

  const {reportsURLs, pageCount} = await scrapeIndex( opts.startAtPageURL )

  const queue = new RequestsQueue( db, scrapeIndex, scrapeReport, urlOfPage, pageCount, reportsURLs, opts.groupSize, opts.groupInterval, opts.pageNumberOfURL(opts.startAtPageURL), opts.startAtReportURI, opts.stopAtReportURI, opts.alreadyScrapedLimit )
  return new ReportStream(queue, urlOfPage)
}

module.exports = scrape
