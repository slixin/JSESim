var express = require('express');
var router = express.Router();
var utils = require('./utils.js');
var moment = require('moment');

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

module.exports = router;
