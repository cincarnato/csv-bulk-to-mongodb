const fs = require('fs');
const csvParser = require('csv-parser');
require('./mongodb')
const Person = require('./PersonModel')


async function processRow(data) {
    return new Promise((resolve, reject) => {


        Person.create(data, function (err, personDoc) {
            if (err) return reject(err);
            resolve(personDoc)
        });

    })

}


function parseCSV() {

    return new Promise((resolve, reject) => {

        var promesasAcumuladas = 0
        var promesasResueltas = 0
        var promesasFallidas = 0
        var promisesList = []

        console.time("CSV")
        const FILE = './people100.csv'
        const stream = fs.createReadStream(FILE)
        const parser = csvParser({separator: ';'})

        stream.pipe(parser)
            .on('data', (row) => {


                //Ejecuto la FUNCION Asyncrona
                promisesList.push(
                    processRow(row).then(personDoc => {
                        promesasResueltas++
                        console.log(JSON.stringify(personDoc) + " PromesasResueltas:" + promesasResueltas.toString())

                        //Verifico si es necesario resumir (Despausar)
                        if (parser.isPaused() && (promesasAcumuladas - promesasResueltas) < 50) {
                            console.log('\x1b[36m%s\x1b[0m', "SE RESUME EL STREAM DIFF:" + (promesasAcumuladas - promesasResueltas))
                            parser.resume()

                        }

                    }).catch(err => {
                        promesasFallidas++
                        console.log('ERROR al insertar registro MONGO:' + row.name + err.message)
                    })
                )

                //Incremento las promesas acumuladas
                promesasAcumuladas++

                //IMPRIMO LAS PROMESAS ACUMULADAS
                console.log("Promesas Acumuladas: " + promesasAcumuladas.toString() + " Resueltas: " + promesasResueltas.toString() + " diff: " + (promesasAcumuladas - promesasResueltas))

                //Verifico si es necesario pausar
                if ((promesasAcumuladas - promesasResueltas) >= 200) {
                    parser.pause()
                    console.warn("SE PAUSA EL STREAM DIFF:" + (promesasAcumuladas - promesasResueltas))
                }

            })
            .on('end', () => {
                console.log("END PARSING FILE");
                console.log("Promesas Acumuladas :" + promesasAcumuladas)
                console.log("Promesas Resueltas  :" + promesasResueltas)


                Promise.all(promisesList).then(() => {
                    console.log("ALL PROMISES ENDED");
                    console.log("Promesas Acumuladas :" + promesasAcumuladas)
                    console.log("Promesas Resueltas  :" + promesasResueltas)
                    console.timeEnd("CSV")
                    resolve({
                        state: 'success',
                        promesasAcumuladas: promesasAcumuladas,
                        promesasResueltas: promesasResueltas,
                        promesasFallidas: promesasFallidas
                    })

                }).catch(e => {
                    reject(e)
                })

            });

    })
}


parseCSV()
    .then(response => {
        console.log("RESULTADO:", response)
    }).catch(
    e => {
        console.error("Error: " + e.message)
    }
)