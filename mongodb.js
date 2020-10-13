const mongoose = require('mongoose');

const MONGO_URI = 'mongodb://127.0.0.1:27017/promesas'

mongoose.connect(MONGO_URI,
    {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        useFindAndModify: false

    })