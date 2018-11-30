var soap = require('soap');
require('dotenv').config();
const fs = require('fs');
var url = 'https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v12/ReportingService.svc?wsdl'
const xml = fs.readFileSync('modules/bing.xml', 'utf-8');

function getBing() {
soap.createClient(url, function (err, client) {
    if (err) {
        console.log("err", err);
    } else {
        client.addSoapHeader({
            'tns:AuthenticationToken': process.env.BING_AUTH_TOKEN,
            'tns:DeveloperToken': process.env.BING_DEV_TOKEN,
            'tns:CustomerId': '36189435',
            'tns:CustomerAccountId': '65093784',
        });       
        client.SubmitGenerateReport(xml, function (err, result) {
            if (err) {
                console.log("err", err.body);
            } else {
                console.log(result);
            }
        });
    }
})
};

module.exports = {
    getBing: getBing
}