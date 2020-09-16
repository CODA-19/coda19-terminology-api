const EventEmitter = require('events')

const _ = require('lodash')
const got = require('got') // https://www.npmjs.com/package/got
const pQueue = require('p-queue').default // https://github.com/sindresorhus/p-queue#interval
const { serializeError } = require('serialize-error')

const MAX_REQS_PER_SEC = 15 // Supposed to be 5, but 10 works.
const RATE_LIMIT_DELAY = 30 * 1000

module.exports = class Airtable extends EventEmitter {
  constructor({
    baseId,
    apiKey,
    tableSettings = [],
    getTableKey = (tableName) => _.camelCase(tableName),
    parseTables = (val) => val,
    restoreFrom = null,
    debug = false,
  }) {
    super()

    Object.assign(this, {
      baseId,
      apiKey,
      tableSettings,
      parseTables,
      getTableKey,
      debug,
    })

    // Support restoring state from a persisted data file
    if (restoreFrom) {
      const { log, lastParsed, fromDate, previousFetch } = restoreFrom
      Object.assign(this, { log, lastParsed, fromDate, previousFetch })
    } else {
      this.log = []
      this.lastParsed = null
      this.fromDate = null
      this.previousFetch = {}
    }

    // use with getter/setter .status
    // types: idle, fetching, parsing, errored (with .err), ratelimited (with .resumesAt)
    this._status = { type: 'idle' }
    this.uid = 0
    this.currentFetch = {}
    this.resetRequested = false
    this.queue = new pQueue({
      concurrency: MAX_REQS_PER_SEC,
      intervalCap: MAX_REQS_PER_SEC,
      interval: 1000,
      timeout: 60 * 1000,
      throwOnTimeout: true,
    })

    this.log.unshift({ at: Date.now(), type: 'ready' })
  }

  get status() {
    return this._status
  }

  set status(val) {
    this._status = val
    this.log.unshift({ uid: this.uid, at: Date.now(), ...val })
    this.emit('status', this.log[0])
    this.uid += 1
  }

  requestReset() {
    this.resetRequested = true
    this.log.unshift({ at: Date.now(), type: 'reset' })
    this.emit('status', this.log[0])
  }

  get collapsedLog() {
    let collapsedLog = this.log.filter((status) => {
      return ['update', 'success', 'errored', 'reset'].includes(status.type)
    })

    let i = 1
    while (i < collapsedLog.length) {
      const sameError =
        collapsedLog[i].type === 'errored' &&
        collapsedLog[i - 1].type === 'errored' &&
        collapsedLog[i].data.err.message ===
          collapsedLog[i - 1].data.err.message

      const sameNoResult =
        collapsedLog[i].type === 'success' &&
        collapsedLog[i].data.totalCount === 0 &&
        collapsedLog[i - 1].type === 'success' &&
        collapsedLog[i - 1].data.totalCount === 0

      if (sameError || sameNoResult) {
        if (!collapsedLog[i - 1].repeatCount) {
          collapsedLog[i - 1].repeatCount = 0
        }

        collapsedLog[i - 1].repeatCount++
        collapsedLog.splice(i, 1)
      } else {
        i++
      }
    }

    return collapsedLog
  }

  async fetchBase() {
    this.status = { type: 'fetching' }

    if (this.resetRequested) {
      this.previousFetch = {}
      this.fromDate = null
      this.resetRequested = false
    }

    this.currentFetch = {}
    const promises = this.tableSettings.map((tableSetting) =>
      this._queuePage(tableSetting)
    )

    try {
      try {
        await Promise.all([...promises, this.queue.onIdle()])
      } catch (err) {
        err.when = 'fetching'
        throw err
      }

      this.fromDate = Airtable.getNewestRecordDate(this.currentFetch)

      let createdCount = 0
      let updatedCount = 0

      for (const [table, records] of Object.entries(this.currentFetch)) {
        if (!this.previousFetch[table]) this.previousFetch[table] = []

        for (const record of records) {
          const existingI = this.previousFetch[table].findIndex(
            (existingRecord) => {
              return existingRecord.id === record.id
            }
          )

          if (existingI >= 0) {
            updatedCount += 1
            this.previousFetch[table][existingI] = record
          } else {
            createdCount += 1
            this.previousFetch[table].push(record)
          }
        }
      }

      const totalCount = createdCount + updatedCount

      if (totalCount > 0 || this.lastParsed === null) {
        this.status = { type: 'parsing' }
        await new Promise((resolve) => setTimeout(resolve, 150))

        try {
          this.lastParsed = await this.parseTables(this.previousFetch)
        } catch (err) {
          err.when = 'parsing'
          throw err
        }
      }

      this.queue.clear()
      this.status = {
        type: 'success',
        data: { totalCount, updatedCount, createdCount },
      }
      this.status = { type: 'idle' }
      return this.lastParsed
    } catch (err) {
      this.queue.clear()
      this.status = { type: 'errored', data: { err: serializeError(err) } }
      throw err
    }
  }

  _queuePage([tableName, query], page = 1, offset = null) {
    return this.queue.add(async () => {
      try {
        const requestURL = `https://api.airtable.com/v0/${
          this.baseId
        }/${encodeURI(tableName)}/`
        const requestQuery = new URLSearchParams(
          Object.entries({
            ...(query || {}),
            ...(offset ? { offset } : {}),
            ...(this.fromDate
              ? {
                  filterByFormula: `IS_AFTER(LAST_MODIFIED_TIME(), '${this.fromDate.toJSON()}')`,
                }
              : {}),
          })
        )

        if (this.debug) {
          console.info(`Table ${tableName} page ${page}`)
          if (this.fromDate)
            console.info(`  From date ${this.fromDate.toJSON()}`)
          console.info(`  ${requestURL}?${requestQuery}\n`)
        }

        const resp = await got(requestURL, {
          headers: { Authorization: `Bearer ${this.apiKey}` },
          searchParams: requestQuery,
          responseType: 'json',
          retry: 30,
        })

        const body = resp.body
        const key = this.getTableKey(tableName)

        if (!this.currentFetch[key]) this.currentFetch[key] = []
        this.currentFetch[key] = this.currentFetch[key].concat(body.records)

        this.emit(
          'progress',
          Object.values(this.currentFetch).flatMap((v) => v).length
        )

        if (body.offset) {
          if (this.debug)
            console.info(
              `Table ${tableName} page ${page}: Got ${body.records.length} records. Queuing next page`
            )
          this._queuePage([tableName, query], page + 1, body.offset)
        } else {
          if (this.debug)
            console.info(
              `Table ${tableName} page ${page}: Done with ${body.records.length} records`
            )
        }
      } catch (err) {
        // Non-got error
        if (!err.response) {
          throw err
        }

        if (err.response.statusCode !== 429) {
          this.queue.clear()

          let combinedError = new Error(
            `Table ${tableName} page ${page}: Failure (status ${err.response.statusCode}: ` +
              `${JSON.stringify(err.response.body || err.response.status)}).`
          )
          combinedError.originalError = err
          throw combinedError
        }

        const resumesAt = Date.now() + RATE_LIMIT_DELAY
        this.status = { type: 'ratelimited', data: { resumesAt } }
        this.queue.pause()

        await new Promise((resolve) => {
          if (this.debug)
            console.info(
              `Pausing queue for ${RATE_LIMIT_DELAY}ms because of rate limiting`
            )
          setTimeout(resolve, RATE_LIMIT_DELAY)
        })

        if (this.debug)
          console.info(`Table ${tableName} page ${page}: Requeuing`)
        this._queuePage([tableName, query], page, offset)

        this.status = { type: 'fetching' }
        this.queue.start()
      }
    })
  }

  static getNewestRecordDate(fetched) {
    return _.chain(fetched)
      .values()
      .flatten()
      .map((r) => _.get(r, 'fields.last_modified', null))
      .compact()
      .map((jsonDate) => new Date(jsonDate).getTime())
      .thru((timestamp) => new Date(Math.max(...timestamp)))
      .thru((date) => (isNaN(date) ? new Date() : date))
      .value()
  }
}
