const express = require("express");
const app = express();
const timer = require('./modules/timer');


const port = 5353;


app.use(express.static('public'));


app.listen(port, () => {
  console.log('Listening on port:', port);
});