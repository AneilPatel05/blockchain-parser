const mongoose = require('mongoose');
mongoose.Promise = require('bluebird');
//const uri = process.env.MONGODB_URI
const uri = process.env.MONGODB_URI||''


const db = mongoose.connect(uri);
const client =  db.connection;
module.exports = client;
