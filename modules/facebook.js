require('dotenv').config();
const bigquery = require('../modules/bigquery');
const pivoter = require('../modules/pivot');
const adsSdk = require('facebook-nodejs-business-sdk');
const AdAccount = adsSdk.AdAccount;
const Campaign = adsSdk.Campaign;

const access_token = process.env.FBTOKEN;
const app_secret = process.env.FB_APP_SECRET;
const app_id = process.env.FB_APP_SECRET;
const api = adsSdk.FacebookAdsApi.init(access_token);
// const showDebugingInfo = true;
// if (showDebugingInfo) {
//   api.setDebug(true);
// }

// const logApiCallResult = (apiCallName, data) => {
//   console.log(apiCallName);
//   if (showDebugingInfo) {
//     console.log('Data:' + JSON.stringify(data));
//   }
// };

let fields, params;

function getData(client) {
    let tableId = 'facebook';
    let datasetId = client.name;
    let campaignIds = [client.facebook_campaign]
    fields = [
        'ad_name',
        'reach',
        'impressions',
        'actions',
        'spend',
        'outbound_clicks',
        'unique_outbound_clicks',
        'clicks',
        'unique_clicks',
        'video_p100_watched_actions'
    ]
    params = {
        'action_attribution_windows': ['28d_view','28d_click'],
        'action_breakdowns': ['action_type'],
        'level' : 'ad',
        'time_increment':  1,
        'filtering': [{ field: 'campaign.id', operator: 'IN', value:  campaignIds }],
        'breakdowns' : ['impression_device', 'publisher_platform'],
        'time_range' : {'since':'2018-10-01','until':'2018-11-01'},
    };
    const insightss = (new AdAccount(client.facebook_act)).getInsights(
    fields,
    params
    )
    .then(res => {
        let data = JSON.stringify(res);
        console.log(data);
    })
    .catch(err => {
        console.log('here is the error', err);
    })    
    // logApiCallResult('insightss api call complete.', insightss);
}
module.exports = {
    getData: getData
}

