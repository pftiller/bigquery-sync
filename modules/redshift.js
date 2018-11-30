const Redshift = require('node-redshift');
const bigquery = require('../modules/bigquery');
require('dotenv').config()
let testClient = {
    user: process.env.TEST_DATABASE_USER,
    password: process.env.TEST_DATABASE_PASSWORD,
    host: process.env.TEST_DATABASE_HOST,
    port: process.env.TEST_DATABASE_PORT,
    database: process.env.TEST_DATABASE
};
let prodClient = {
    user: process.env.PROD_DATABASE_USER,
    password: process.env.PROD_DATABASE_PASSWORD,
    host: process.env.PROD_DATABASE_HOST,
    port: process.env.PROD_DATABASE_PORT,
    database: process.env.PROD_DATABASE
};

let redshift = new Redshift(prodClient);

function getRedshiftData(account, aw_query, ttd_query, combined_query) {
    let result={};
    return new Promise(function (resolve) {
        console.log(`getting ${account.name} data from Redshift`)
        let datasetId = account.name
        redshift.query(aw_query, {
                raw: true
            })
            .then(function (aw) {
                let tableId = 'adwords';
                result.payload = aw;
                bigquery.dataToBigQuery(result, datasetId, tableId)
            })
        redshift.query(ttd_query, {
                raw: true
            })
            .then(function (ttd) {
                let tableId = 'trade_desk';
                result.payload = ttd;
                bigquery.dataToBigQuery(result, datasetId, tableId)
            })
        redshift.query(combined_query, {
                raw: true
            })
            .then(function (combined) {
                let tableId = 'combined';
                result.payload = combined;
                bigquery.dataToBigQuery(result, datasetId, tableId);
            })
    })
}
module.exports = {
    getRedshiftData: getRedshiftData
}