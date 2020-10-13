const randomString = require("./helpers/randomString");
const randomNumber = require("./helpers/randomNumber");
const fs = require('fs')
const CSV_FILE = "people100.csv"
function generateFileSync(rows) {
    console.time("sincrono")

    //HEADER
    let columns = "name;lastname;dni"
    fs.appendFileSync(CSV_FILE, columns + '\n');

    //GENERATE ROWS
    for (let i = 0; i < rows; i++) {
        let row = randomString(4) + ';' + randomString(6) + ';' + randomNumber(8) + '\n'
        fs.appendFileSync(CSV_FILE, row);
        console.log("row number: " + i + " row data: " + row)
    }

    console.timeEnd("sincrono")
}


generateFileSync(100)