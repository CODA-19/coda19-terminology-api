const fs = require('fs')
const GSR = require('google-search-results-nodejs')

SERPAPI_KEY = 'ADD KEY HERE'

async function searchApi(query) {
  
  const client = new GSR.GoogleSearchResults(SERPAPI_KEY)

  const params = {
    engine: 'google',
    q: query,
    google_domain: 'google.com',
    gl: 'us',
    hl: 'en'
  }

  return new Promise((resolve, reject) => {
    client.json(params, resolve)
  })
  
}

async function search(query) {
  
  try {
    let results = await searchApi(query)
    if (results.search_information && 
        results.search_information.organic_results_state == 
        'Empty showing fixed spelling results') {
      return []
    }
    return results.organic_results
  } catch (err) {
    console.log(err)
    return {}
  }
}

async function main(inFile, outFile) {
  
  let loincElements = JSON.parse(fs.readFileSync(inFile, 'utf8')).map((d) => d.trim())
  
  for (let loincElement of loincElements) {
      
    let bodySite
      
    if (loincElement.includes('mict') || loincElement.includes('urin')) {
      bodySite = 'urine'
    } else {
      bodySite = 'blood'
    }
    
    let results = await search(`${loincElement} ${bodySite} site: https://loinc.org`)
    
    let url
    let title
    
    if (results.length > 0) {
      title = results[0].title
      url = results[0].link
    } else {
      snippet = ''
      url = ''
    }
    
    let entry = [loincElement, url, title].join('\t')
    
    let file = fs.readFileSync(outFile)
    file = file + '\n' + entry
    fs.writeFileSync(outFile, file)

    console.log(results[0])
  }
  
}

main('./lab_names.json', './labs_mapped.csv')