const fs = require('fs');
const csvParser = require('csv-parser');
require('./mongodb')
const Person = require('./PersonModel')
const FILE = './people200034.csv'


async function processRows(chunkRows) {
    return new Promise((resolve, reject) => {

        Person.insertMany(chunkRows, function (err, personDocs) {
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

                            console.log(
                                "Chunk number: ", chunkCounter,
                                "Chunk Length: ", chunkRows.length,
                                "Rows Counter: ", rowsCounter)

                            chunkRows = []
                            parser.resume()

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

                            console.log(
                                "Chunk number: ", chunkCounter,
                                "Chunk Length: ", chunkRows.length,
                                "Rows Counter: ", rowsCounter)

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