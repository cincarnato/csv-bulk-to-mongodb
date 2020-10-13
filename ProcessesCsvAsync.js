const fs = require('fs');
const csv = require('csv-parser');

// Create a stream from some character device.
const stream = fs.createReadStream('./people2M.csv')

var promesasAcumuladas = 0
var promesasResueltas = 0
var promisesList = []

async function processRow(data) {
    return new Promise((resolve, reject) => {
        promesasAcumuladas++
        setTimeout(() => {
            resolve(data)
        }
        , 1700)
    })

}

console.time("CSV")

function parseCSV() {

    return new Promise((resolve,reject) => {

        stream.pipe(csv({separator: ';'}))
            .on('data', (data) => {

                //IMPRIMO LAS PROMESAS ACUMULADAS
                console.log("Promesas Acumuladas: " + promesasAcumuladas)

                //Ejecuto la FUNCION Asyncrona
                promisesList.push(
                    processRow(data).then(response => {
                        promesasResueltas++
                        console.log(JSON.stringify(response) + " PromesasAcumuladas:" + promesasResueltas)
                    })
                )


            })
            .on('end', () => {
                console.log("END PARSING FILE");
                console.log("Promesas Acumuladas :" + promesasAcumuladas)
                console.log("Promesas Resueltas  :" + promesasResueltas)
                console.timeEnd("CSV")




                Promise.all(promisesList).then(() => {
                    console.log("ALL PROMISES ENDED");
                    console.log("Promesas Acumuladas :" + promesasAcumuladas)
                    console.log("Promesas Resueltas  :" + promesasResueltas)

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