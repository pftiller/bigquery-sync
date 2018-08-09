// Aoo setup
const express = require("express");
const app = express();
const redshift = require('./modules/redshift');

const port = 5353;


app.use(express.static('public'));


app.listen(port, () => {
    console.log('Listening on port:', port);
  });

