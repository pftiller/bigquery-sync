const Redshift = require('node-redshift');
const cron = require("node-cron");
const bigquery = require('../modules/bigquery');
const client = require('../modules/pool');
var redshift = new Redshift(client, {
    rawConnection: true
});

function getRedshiftData() {
    return new Promise(function(resolve, reject) {
        redshift.connect(function(err, data) {
            if (err) {
                reject(err);
            } else {
                redshift.query('SELECT * FROM "adwords"', {
                        raw: true
                    })
                    .then(function(aw) {
                        data.adwords = aw
                    }),
                    redshift.query('SELECT * FROM "trade_desk"', {
                        raw: true
                    })
                    .then(function(ttd) {
                        data.trade_desk = ttd
                        if (data.trade_desk && data.adwords) {
                            resolve((data));
                        }
                    })
            }
        })

    })
}
cron.schedule('30 * * * * *', () => {
    var redshiftPromise = getRedshiftData();
    redshiftPromise.then(function(result) {
        redshiftData = result;
        console.log("finished job");
        bigquery.dataToBigQuery(redshiftData)
    }, function(err) {
        console.log(err);
    })
});


module.exports = redshift;