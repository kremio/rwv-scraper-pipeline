class ReportParserError extends Error {
  constructor(error, reportURI){
    super(error.message)
    this.caughtError = error
    this.reportURI = reportURI
  }
}

module.exports = {
  ReportParserError
}
