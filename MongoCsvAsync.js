const fs = require('fs');
const csv = require('csv-parser');
require('./mongodb')
const Person = require('./PersonModel')



var promesasAcumuladas = 0
var promesasResueltas = 0
var promisesList = []

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
        console.time("CSV")
        const FILE = './people2M.csv'
        const stream = fs.createReadStream(FILE)
        const parser = csv({separator: ';'})
        stream.pipe(parser)
            .on('data', (data) => {



                //Ejecuto la FUNCION Asyncrona
                promisesList.push(
                    processRow(data).then(personDoc => {
                        promesasResueltas++
                        console.log(JSON.stringify(personDoc) + " PromesasResueltas:" + promesasResueltas.toString())


                        //Verifico si es necesario resumir
                        if(stream.isPaused() && (promesasAcumuladas - promesasResueltas) < 50){
                            console.log( '\x1b[36m%s\x1b[0m', "SE RESUME EL STREAM DIFF:" + (promesasAcumuladas - promesasResueltas))
                            parser.resume()

                        }

                    })
                )

                //Incremento las promesas acumuladas
                promesasAcumuladas++

                //IMPRIMO LAS PROMESAS ACUMULADAS
                console.log("Promesas Acumuladas: " + promesasAcumuladas.toString() + " Resueltas: " + promesasResueltas.toString() + " diff: "+ (promesasAcumuladas - promesasResueltas))

                //Verifico si es necesario pausar
                if( (promesasAcumuladas - promesasResueltas) >= 200){
                    parser.pause()
                    console.warn("SE PAUSA EL STREAM DIFF:" + (promesasAcumuladas - promesasResueltas) )
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
                        promesasResueltas: promesasResueltas
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
    }).catch(e => {
    console.error(e)
})