const Redshift = require('node-redshift');
const bigquery = require('../modules/bigquery');
require('dotenv').config()
let client = {
    user: process.env.USER,
    host: process.env.HOST,
    port: process.env.PORT,
    database: process.env.DATABASE
};
let redshift = new Redshift(client);

function getRedshiftData(account, aw_query, ttd_query, combined_query) {
    let result={};
    return new Promise(function (resolve) {
        console.log(`getting ${account.name} data from Redshift`)
        let datasetId = account.name
        redshift.query(aw_query, {
                raw: true
            })
            .then(function (aw) {
                let tableId = 'vw_search_account_performance';
                result.payload = aw;
                bigquery.dataToBigQuery(result, datasetId, tableId)
            })
        redshift.query(ttd_query, {
                raw: true
            })
            .then(function (ttd) {
                let tableId = 'vw_ttd_performance';
                result.payload = ttd;
                bigquery.dataToBigQuery(result, datasetId, tableId)
            })
        redshift.query(combined_query, {
                raw: true
            })
            .then(function (combined) {
                let tableId = 'vw_combined_performance';
                result.payload = combined;
                bigquery.dataToBigQuery(result, datasetId, tableId)
                console.log(`finished ${data.name} upload`)
                setTimeout(function () {
                    resolve('success');
                    //reject('error');
                }, 3000);
            })
    })
}
module.exports = {
    getRedshiftData: getRedshiftData
}