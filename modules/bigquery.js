function dataToBigQuery(data) {
    console.log('bigquery function has been called');
    const BigQuery = require('@google-cloud/bigquery');
    const bigquery = new BigQuery({
        projectId: 'tc-data-warehouse',
        credentials: require('../keyfile')
      });
    const dataset = bigquery.dataset("sample");
  

    // Inserts data into adwords
    dataset.table('adwords').insert(data.adwords)
      .then(() => {
        console.log(`Inserted ${data.adwords.length} rows into adwords`);
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
    dataToBigQuery:dataToBigQuery
};