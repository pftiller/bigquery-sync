const Redshift = require('node-redshift');
const CronJob = require('cron').CronJob;
const bigquery = require('../modules/bigquery');
// const client = require('../modules/pool');
var client = {
    user: process.env.USER,
    password: process.env.DATABASE_SECRET,
    host: process.env.HOST,
    port: process.env.PORT ,
    database: process.env.DATABASE
};
var redshift = new Redshift(client, {
    rawConnection: true
});

function getRedshiftData() {
    return new Promise(function(resolve, reject) {
        let data = {};
        redshift.connect(function(err) {
            if (err) {
                reject(err);
            } else {
                redshift.query('SELECT * FROM "adwords"', {
                        raw: true
                    })
                    .then(function(aw) {
                        let tableId = 'adwords';
                        data.payload = aw;
                        bigquery.dataToBigQuery(data, tableId)
                    }),
                    redshift.query('SELECT * FROM "trade_desk"', {
                        raw: true
                    })
                    .then(function(ttd) {
                        let tableId = 'trade_desk';
                        data.payload = ttd;
                        bigquery.dataToBigQuery(data, tableId);
                        if (data.payload) {
                            redshift.close();
                            resolve('all done');
                        }
                    })
           
                }
        })

    })
}
new CronJob('30 * * * * *', function()  {
    console.log('starting job');
    var redshiftPromise = getRedshiftData();
    redshiftPromise.then(function(response) {
        console.log(response);
    }, function(err) {
        console.log(err);
    })
}, null, true, 'America/Chicago');


module.exports = redshift;