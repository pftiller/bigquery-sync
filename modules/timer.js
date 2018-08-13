const CronJob = require('cron').CronJob;
const accounts = require('./accounts');
let accountList = accounts;
const facebook = require('./facebook');


  new CronJob('30 * * * * *', function () {
    console.log('starting job');
    for (let i = 0; i < accountList.length; i++) {
      let account = accounts[i];
      console.log(`Beginning ${account.name}`);
      let queries = require('./queries');
      queries.receiveIds(account);
      facebook.getData(account);
    }
  }, null, true, 'America/Chicago')