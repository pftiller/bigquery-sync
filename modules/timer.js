const CronJob = require('cron').CronJob;
const accounts = require('./accounts');


  new CronJob('30 * * * * *', function () {
    console.log('starting job');
    for (account of accounts) {
      console.log(`Beginning ${account.name}`);
      // let queries = require('./queries');
      // queries.receiveIds(account);
      const facebook = require('./facebook');
      facebook.getData(account);
      // let bing = require('./bing');
      // bing.getBing();
    }
  }, null, true, 'America/Chicago')