function dataToBigQuery(data, tableId) {

  console.log('bigquery function has been called');
  const BigQuery = require('@google-cloud/bigquery');
  const bigquery = new BigQuery({
      projectId: 'tc-data-warehouse',
      credentials: require('../keyfile')
  });
let table = bigquery.dataset('sample').table(tableId);

table.insert(data.payload)
  .then(() => {
    console.log(`Inserted ${data.payload.length} rows into ${tableId}`);
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
  dataToBigQuery: dataToBigQuery
};
