const fs = require('fs');
const csv = require('csv-parser');

// Create a stream from some character device.
const stream = fs.createReadStream('./people2M.csv')

async function processRow(data){
    console.log(data)
}

console.time("CSV")

stream.pipe(csv({separator: ';'}))
    .on('data', (data) => processRow(data))
    .on('end', () => {
        console.log("END");
        console.timeEnd("CSV")
    });