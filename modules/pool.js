var pg = require('pg');

var config = {
    user: process.env.USER,
    password: process.env.DATABASE_SECRET,
    host: process.env.HOST,
    port: process.env.PORT ,
    database: process.env.DATABASE
};


module.exports = new pg.Pool(config);
