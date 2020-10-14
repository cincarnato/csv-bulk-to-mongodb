const fs = require('fs');
const csvParser = require('csv-parser');
require('./mongodb')
const Person = require('./PersonModel')
const FILE = './people200034.csv'


async function processRows(chunkRows) {
    return new Promise((resolve, reject) => {

        Person.insertMany(chunkRows,{ordered:false}, function (err, personDocs) {
            if (err) return reject(err);
            resolve(personDocs)
        });

    })

}


function parseCSV(file) {

    return new Promise((resolve, reject) => {


        console.time("CSV")

        const stream = fs.createReadStream(file)
        const parser = csvParser({separator: ';'})
        const LIMIT = 100
        let chunkRows = []
        let chunkCounter = 0
        let rowsCounter = 0
        let chucksFallidos= 0
        let rowsInserted = 0
        let rowsFailed = 0
        let promises = []

        stream.pipe(parser)
            .on('data', (row) => {


                //Preparo el CHUNK
                chunkRows.push(row)
                rowsCounter++

                //Verifico si el CHUNK llego al limite
                if (chunkRows.length >= LIMIT) {
                    parser.pause()
                    chunkCounter++


                    promises.push(
                        processRows(chunkRows).then(personDocs => {
                            rowsInserted += personDocs.length
                            rowsFailed += (chunkRows.length - personDocs.length)
                            console.log(
                                "Chunk number: ", chunkCounter,
                                "Chunk Length: ", chunkRows.length,
                                "Rows Counter: ", rowsCounter,
                                "Rows Inserted: ", rowsInserted,
                                "Rows Failed: ",rowsFailed)

                            chunkRows = []
                            parser.resume()

                        })
                        .catch(err => {
                        chucksFallidos++
                        console.log('ERROR al insertar registro MONGO:' + row.name + err.message)
                        })
                    )

                }

            })
            .on('end', () => {

                console.log("PARSED ENDED")

                if(chunkRows.length > 0){
                    chunkCounter++
                    promises.push(
                        processRows(chunkRows).then(personDocs => {

                            rowsInserted += personDocs.length
                            rowsFailed += (chunkRows.length - personDocs.length)
                            console.log(
                                "Chunk number: ", chunkCounter,
                                "Chunk Length: ", chunkRows.length,
                                "Rows Counter: ", rowsCounter,
                                "Rows Inserted: ", rowsInserted,
                                "Rows Failed: ",rowsFailed)

                            chunkRows = []

                        })
                    )


                }

                Promise.all(promises).then(() => {
                    resolve({
                        state: 'success',
                        rowsCounter: rowsCounter,
                        chunkCounter: chunkCounter
                    })
                })

            })


    })
}

console.time("CSV")
parseCSV(FILE)
    .then(response => {
        console.timeEnd("CSV")
        console.log("RESULTADO:", response)
       process.exit(1)
    }).catch(e => {
    console.error(e)
})