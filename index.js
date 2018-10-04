const csv = require('csvtojson')
const fs = require('fs')

const ignoredIds = ['3427285', '242841733']
const placeholders = ['${newline}']
const languageCodes = ['en', 'fr', 'de']
const mainLanguageCode = 'en'
const blockSize = 10000

const readLanguageFile = languageCode => csv().fromFile(`./resources/${languageCode}.lang.csv`)

const removeIgnoredIds = entries => entries.filter(entry => !ignoredIds.includes(entry.ID))

const replaceNewLineCodesWithPlaceholder = text => text.replace(/\\n\\n/g, ' ${newline} ').replace(/\\r\\n/g, ' ${newline} ')

const processLanguage = (languageCode, entries) => {
  let processedEntries = removeIgnoredIds(entries).map(entry => {
    entry.Text = replaceNewLineCodesWithPlaceholder(entry.Text)
    return entry
  })
  return { code: languageCode, entries: processedEntries }
}

const findEquivalentEntryWords = (processedLanguage, referenceEntry) => {
  let equivalentEntry = processedLanguage.entries.find(entry => entry.ID === referenceEntry.ID && entry.Index === referenceEntry.Index && entry.Unknown === referenceEntry.Unknown)
  return equivalentEntry ? { code: processedLanguage.code, words: splitWords(equivalentEntry.Text) } : null
}

const getOtherLanguagesEquivalentEntryWords = (languages, referenceEntry) => {
  return languages.reduce((acc, language) => {
    let equivalentEntry = findEquivalentEntryWords(language, referenceEntry)
    if (equivalentEntry) {
      acc.push(equivalentEntry)
    }
    return acc
  }, [])
}

const splitWords = text => text.replace(/[.,…]/g, '').split(' ').filter(word => word !== '' && !placeholders.includes(word))

const getOtherLanguagesEquivalentWords = (languages, referenceEntry) => {
  let referenceWords = splitWords(referenceEntry.Text)
  let equivalentLanguagesEntryWords = getOtherLanguagesEquivalentEntryWords(languages, referenceEntry)
  let sameWords = referenceWords.reduce((acc, word) => {
    let hasTheSameWordInAllOtherLangues = equivalentLanguagesEntryWords.every(equivalentLanguageEntry => equivalentLanguageEntry.words.includes(word))
    if (hasTheSameWordInAllOtherLangues) {
      acc.push(word)
    }
    return acc
  }, [])
  return sameWords.length !== referenceWords.length && sameWords.length > 0 ? sameWords : []
}
let jobStart = process.hrtime()
let filesReadStart = process.hrtime()
let processingStart
console.log(`Job started. Reading ${languageCodes.join(',')} language files`)
let promises = languageCodes.map(languageCode => readLanguageFile(languageCode).then(processLanguage.bind(null, languageCode)))
Promise.all(promises).then(languages => {
  let filesReadEnd = process.hrtime(filesReadStart)
  console.log(`Reading files complete. Took ${filesReadEnd[0]} seconds.`)
  let mainLanguage = languages.find(language => language.code === mainLanguageCode)
  let otherLanguages = languages.filter(language => language.code !== mainLanguageCode)
  processingStart = process.hrtime()
  console.log(`Starting first bock processing. Block size is ${blockSize}.`)
  let blockStart = process.hrtime()
  let entitiesDictionary = mainLanguage.entries.reduce((acc, entry, index) => {
    let dicionaryEntries = acc.concat(getOtherLanguagesEquivalentWords(otherLanguages, entry))
    if ((index + 1) % blockSize === 0) {
      let blockEnd = process.hrtime(blockStart)
      console.log(`Processed block with ending line number ${index + 1}. Took ${blockEnd[0]} seconds.`)
      blockStart = process.hrtime()
    }
    return dicionaryEntries
  }, [])
  let uniqueEntitiesDictionary = [...new Set(entitiesDictionary)]
  return uniqueEntitiesDictionary
}).then(dictionary => {
  let processingEnd = process.hrtime(processingStart)
  console.log(`Processing blocks finished. Took ${(processingEnd[0] / 60).toFixed(0)} minutes`)
  fs.writeFileSync('./entidades_eso.csv', dictionary.join('\n'))
  let jobEnd = process.hrtime(jobStart)
  console.log(`Job finished. Found ${dictionary.length} entities. Took ${(jobEnd[0] / 60).toFixed(0)} minutes.`)
})
