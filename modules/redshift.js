const Redshift = require('node-redshift');
const queries = require('./queries');
const bigquery = require('../modules/bigquery');
let client = {
    user: process.env.USER,
    host: process.env.HOST,
    port: process.env.PORT,
    database: process.env.DATABASE
};
let redshift = new Redshift(client);

function getRedshiftData(account, aw_query, ttd_query, combined_query) {
    // return new Promise(function (resolve, reject) {
        console.log(`getting ${account.name} data from Redshift`)
        redshift.query(aw_query, {
                raw: true
            })
            .then(function (aw) {
                let tableId = 'vw_search_account_performance';
                result.payload = aw;
                bigquery.dataToBigQuery(result, tableId)
            })
        redshift.query(ttd_query, {
                raw: true
            })
            .then(function (ttd) {
                let tableId = 'vw_ttd_performance';
                result.payload = ttd;
                bigquery.dataToBigQuery(result, tableId)
            })
        // redshift.query(combined_query, {
        //         raw: true
        //     })
        //     .then(function (combined) {
        //         let tableId = 'vw_combined_performance';
        //         result.payload = combined;
        //         bigquery.dataToBigQuery(result, tableId)
        //         console.log(`finished ${data.name} upload`)
                // setTimeout(function () {
                //     resolve('success');
                //     //reject('error');
                // }, 3000);
            // })
    // })
}

// function getRedshiftData() {
//     return new Promise(function (resolve, reject) {
//         redshift.connect(function (err) {
//             if (err) {
//                 reject(err);
//             } else {
//                
//                 for (account in accounts.adwords) {
//                     queries.adwords.id = account.adwords;
//                     queries.trade_desk.id = account.trade_desk;
//                     redshift.query(queries.adwords.query, {
//                             raw: true
//                         })
//                         .then(function (aw) {
//                             let tableId = 'adwords';
//                             data.payload = aw;
//                             bigquery.dataToBigQuery(data, tableId)
//                         }),
//                         redshift.query(queries.trade_desk.query, {
//                             raw: true
//                         })
//                         .then(function (ttd) {
//                             let tableId = 'trade_desk';
//                             data.payload = ttd;
//                             bigquery.dataToBigQuery(data, tableId)
//                         }),
//                         redshift.query(queries.combined.query, {
//                             raw: true
//                         })
//                         .then(function (combined) {
//                             let tableId = 'combined';
//                             data.payload = combined;
//                             bigquery.dataToBigQuery(data, tableId);
//                             if (data.payload) {
//                                 redshift.close();
//                                 resolve('all done');
//                             }
//                         })
//                 }
//             }
//         })
//     })
// }



module.exports = {
    getRedshiftData: getRedshiftData
}