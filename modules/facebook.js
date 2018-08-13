const adsSdk = require('facebook-nodejs-business-sdk');
const AdAccount = adsSdk.AdAccount;
const AdsInsights = adsSdk.AdsInsights;
require('dotenv').config()
let access_token = process.env.FBTOKEN;
const api = adsSdk.FacebookAdsApi.init(access_token);
const account = new AdAccount('act_1331028070365251');
const showDebugingInfo = true; // Setting this to true shows more debugging info.
if(showDebugingInfo) {
    api.setDebug(true);
}



let ads_insights;
let ads_insights_id;


function getData() {
    console.log('inside receiveAcct');
    const fields = [
    'impressions',
    ];
    const params = {
    'level' : 'campaign',
    'filtering' : [],
    'breakdowns' : ['ad_id'],
    'time_range' : {'since':'2018-07-01','until':'2018-08-01'},
    };
    account.getInsights(
    fields,
    params
    )
    .then((result) => {
    logApiCallResult('ads_insights api call complete.', result);
    ads_insights_id = result[0].id;
    })
    .catch((error) => {
    console.log(error);
    });
}

//     let ad_account_id = '';
//     let account = new AdAccount(ad_account_id);
//     account.read([AdAccount.Fields.name, AdAccount.Fields.age])
//     .then((result) => {
//             logApiCallResult('ads_insights api call complete.', result);
//             ads_insights_id = result[0].id;
//         })
//         .catch((error) => {
//             console.log(error);
//         });
// };
   

   

  

  const logApiCallResult = (apiCallName, data) => {
    console.log(apiCallName);
        if (showDebugingInfo) {
            console.log('Data:' + JSON.stringify(data));
            }
        };

     
module.exports = {
    getData: getData
}

