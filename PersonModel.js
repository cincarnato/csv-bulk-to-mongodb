const mongoose = require('mongoose');

const Schema = mongoose.Schema;

const personSchema = new Schema({

    name: {type: String, required: false},
    lastanme: {type: String, required: false},
    dni: {type: Number, required: false},
});

const person = mongoose.model('Person', personSchema);

module.exports = person;