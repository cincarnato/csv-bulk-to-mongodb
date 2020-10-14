const mongoose = require('mongoose');

const Schema = mongoose.Schema;

const personSchema = new Schema({

    name: {type: String, required: false},
    lastname: {type: String, required: false},
    dni: {
        type: String,
        required: false,
        validate: {
            validator: function (value) {
                let r = /^0/;
                return !r.test(value);
            },
            message: "DNI no puede empezar con 0"
        }
    },
});

const person = mongoose.model('Person', personSchema);

module.exports = person;