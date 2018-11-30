const BigQuery = require('@google-cloud/bigquery');


function dataToBigQuery(data, datasetId, tableId) {
  console.log('dataToBigQuery function has been called');
  const bigquery = new BigQuery({
      projectId: 'tc-data-warehouse',
      credentials: require('../keyfile')
  });
let table = bigquery.dataset(datasetId).table(tableId);

table.insert(data.payload)
  .then(() => {
    console.log(`Inserted into ${tableId}`);
  })
  .catch(err => {
    if (err && err.name === 'PartialFailureError') {
      if (err.errors && err.errors.length > 0) {
        console.log('Insert errors:');
        err.errors.forEach(err => console.error(err));
      }
    } else {
      console.error('ERROR:', err);
    }
  });
}
function fbToBigQuery(data, datasetId, tableId) {
  console.log('fbToBigQuery function has been called');
  const bigquery = new BigQuery({
      projectId: 'tc-data-warehouse',
      credentials: require('../keyfile')
  });
let table = bigquery.dataset(datasetId).table(tableId);

table.insert(data.data)
  .then(() => {
    console.log(`Inserted into ${tableId}`);
  })
  .catch(err => {
    if (err && err.name === 'PartialFailureError') {
      if (err.errors && err.errors.length > 0) {
        console.log('Insert errors:');
        err.errors.forEach(err => console.error(err));
      }
    } else {
      console.error('ERROR:', err);
    }
  });
}
module.exports = {
  dataToBigQuery: dataToBigQuery,
  fbToBigQuery: fbToBigQuery
};
