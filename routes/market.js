var express = require('express');
var router = express.Router();
var utils = require('./utils.js');
var moment = require('moment');
var MarketManager = require('./marketManager.js');
var csv = require('csvtojson');
var path = require('path');

router.post('/', function(req, res, next) {
    if (global.market == null) {
        res.status(400).send({ error: 'No Market be found'});
    } else {
        res.send(global.market.config);
    }
});

router.post('/instruments', function(req, res, next) {
    var file = req.body.file;

    var filepath = path.resolve("./routes/instrument/"+file);
    var instruments = [];
    csv().fromFile(filepath)
        .on('json',(jsonObj)=>{
            instruments.push(jsonObj);
        })
        .on('done',(error)=>{
            res.send(instruments);
        });
});

router.post('/news', function(req, res, next) {
    var urgency = req.body.urgency;
    var headline = req.body.headline;
    var text = req.body.text;
    var instruments = req.body.instruments;
    var underlyings = req.body.underlyings;
    var firmlist = req.body.firmlist;
    var userlist = req.body.userlist;

    if (global.market == undefined) {
        res.status(400).send({ error: 'No Market be found'});
    } else {
        var marketManager = global.market.instance;
        var news = {
            "urgency": urgency,
            "headline": headline,
            "text": text,
            "instruments": instruments,
            "underlyings": underlyings,
            "firmlist": firmlist,
            "userlist": userlist
        }
        marketManager.publishNews(news, function(err) {
            if (err) res.status(400).send( { error: err });
            else res.send({ });
        });
    }
});

router.post('/reset', function(req, res, next) {
    var gateway = req.body.gateway;
    var client = req.body.client;
    var data = JSON.parse(req.body.data);

    var gt = null;
    if (global.market == undefined) {
        res.status(400).send({ error: 'No Market be found'});
    } else {
        var marketManager = global.market.instance;

        switch(gateway) {
            case 'orderentry':
                gt = marketManager.orderentry;
                break;
            case 'dropcopy':
                gt = marketManager.dropcopy;
                break;
            case 'posttrade':
                gt = marketManager.posttrade;
                break;
        }

        if (gt.clients.has(client)) {
            gt.clients.get(client).session.modifyBehavior(data);
            res.send({});
        } else {
            res.status(400).send({ error: 'No client '+ client + ' on gateway: '+ gateway });
        }
    }
});

router.post('/start', function(req, res, next) {
    var market = req.body.market;

    if (market == null) {
        res.status(400).send({ error: 'No Market be found'});
    } else {
        var marketManager = new MarketManager(market);
        marketManager.start(function(err) {
            if (err) res.status(400).send( { error: err });
            else {
                market.isrunning = true;
                global.market = { config: market, instance: marketManager };
                res.send({});
            }
        });
    }
});

router.post('/stop', function(req, res, next) {
    if (global.market == undefined) {
        res.status(400).send({ error: 'No Market be found'});
    } else {
        var marketManager = global.market.instance;
        marketManager.stop(function(err) {
            if (err) res.status(400).send( { error: err });
            else {
                global.market = null;
                res.send({ });
            }
        });
    }
});


module.exports = router;
