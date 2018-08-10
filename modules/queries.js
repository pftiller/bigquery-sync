const redshift = require('./redshift');
let aw_query;
let ttd_query;
let combined_query;

let receiveIds = function(account) {

aw_query = `SELECT * FROM vw_search_account_performance WHERE account_id=${account.adwords}`;

ttd_query = `SELECT * FROM vw_ttd_performance WHERE advertiser_id='${account.trade_desk}'`;

// combined_query = `SELECT *
//                 FROM
//                 (SELECT
//                     DISTINCT statistic_dt,
//                     creative_typ,
//                     NULL AS source_system_nm,
//                     0 AS video_view_cnt,
//                     player_views_cnt,
//                     impressions_cnt,
//                     0 AS impression_cnt,
//                     clicks_cnt,
//                     0 AS click_cnt,
//                     media_cost_usd_amt,
//                     0 AS cost_amt 
//                 FROM
//                     vw_ttd_performance 
//                 WHERE
//                     advertiser_id=${account.trade_desk}
//                 UNION
//                 ALL SELECT
//                     DISTINCT statistic_dt,
//                     NULL AS creative_typ,
//                     source_system_nm,
//                     video_view_cnt,
//                     0 AS player_views_cnt,
//                     0 AS impressions_cnt,
//                     impression_cnt,
//                     0 AS clicks_cnt,
//                     click_cnt,
//                     0 AS media_cost_usd_amt,
//                     cost_amt 
//                 FROM
//                     vw_search_account_performance 
//                 WHERE
//                     account_id=${account.adwords} 
//                 ) data 
//                 GROUP BY
//                 data.statistic_dt,
//                 data.creative_typ,
//                 data.source_system_nm 
//                 ORDER BY
//                 statistic_dt`;

       redshift.getRedshiftData(account, aw_query, ttd_query, combined_query)      
}

module.exports = {
    aw_query:aw_query,
    ttd_query:ttd_query,
    combined_query:combined_query,
    receiveIds:receiveIds
}
