const fs = require('fs')
const Airtable = require('./Airtable')

const siteNames = ['CHUM', 'CISSSCA', 'CHUQ']

class Fetcher extends Airtable {
  constructor(...args) {
    super(...args)
    this.tableSettings = [
      ["PCRName", {}],
      ["PCRResultStatus", {}],
      ["PCRSite", {}],
      ["LabSite", {}],
      ["LabName", {}],
      ["ObservationNameUnit", {}],
      ["CultureName", {}],
      ["CultureResultStatus", {}],
      ["DrugName", {}],
      ["DrugRoute", {}],
      ["DrugCodes", {}],
      ["DrugFrequency", {}],
      ["UnitType", {}],
      ["ImagingName", {}]
    ]
  }
}

const getSiteMappings = async (siteName) => {

  const fetcher = new Fetcher({ 
    baseId: 'appKxBkg0yI3NVqkz', 
    apiKey: 'keyggEJjl4atn9Mkd'
  })

  const tables = await fetcher.fetchBase()
  const filteredTables = {}
  
  for (let tableName in tables) {
    let table = tables[tableName]
    
    filteredTables[tableName] = table.filter( (rec) => 
      (rec.fields.site && ( rec.fields.site.includes(siteName) || rec.fields.site.includes('ALL') )  ))
  }
  
  const mappedTables = {}
  
  for (let tableName in filteredTables) {
    let table = filteredTables[tableName]
    mappedTables[tableName] = table.map((row) => {
      let mappedRow = {}
      for (let field in row.fields) {
        if (field == 'raw_string_lower') {
          mappedRow[field] = row.fields[field].toLowerCase()
        } else if (field == 'site') {
          mappedRow[field] = siteName
        } else {
          mappedRow[field] = row.fields[field]
        } 
      }
      return mappedRow
    })
  }
  
  return mappedTables
  
}

const getAllMappings = async (siteName) => {
  for (let siteName of siteNames) {
    const siteJson = await getSiteMappings(siteName)
    fs.writeFileSync(`./dictionaries/${siteName}.json`, 
      JSON.stringify(siteJson, null, 2))
  }
}

getAllMappings()

