var express = require('express');
var app = express();

const timer = require('./modules/timer');

const port = 5032;

app.use(express.static('public'));


app.listen(port, () => {
  console.log('Listening on port:', port);
});