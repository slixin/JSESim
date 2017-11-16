var express = require('express');
var router = express.Router();
var utils = require('./utils.js');
var moment = require('moment');
var JseClient = require('nodejse').JseClient;

router.post('/logon', function(req, res, next) {
    var host = req.body.host;
    var port = req.body.port;
    var username = req.body.username;
    var password = req.body.password;
    var newpassword = req.body.newpassword;
    var options = req.body.options == undefined ? {} : JSON.parse(req.body.options);
    var spec = req.body.spec;

    utils.getDictionary(spec, function(err, dictionary) {
        if (err) log.error(err);
        else {
            global.client = new JseClient(host, port, dictionary, username, password, newpassword, options);
            global.client.createConnection(function(err, session) {
                if (err) {
                    log.error(err);
                    res.status(400).send({ error: err });
                } else {
                    session.on('outmsg', function(outmsg) {
                        var message = outmsg.message;
                        var msgtype = message['MsgType'];

                        if (msgtype == "0")
                            log.debug("- OUT\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                        else
                            log.info("- OUT\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");

                        if (client.storemsg) {
                            if (msgtype != "0") {
                                global.client.messages.unshift({
                                    json: message,
                                    time: moment().now(),
                                    direction: 1
                                });
                            }
                        }
                    });

                    session.on('msg', function(msg) {
                        var message = msg.message;
                        var msgtype = message['MsgType'];

                        if (msgtype == "0")
                            log.debug("- IN\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                        else
                            log.info("- IN\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");

                        if (global.client.storemsg) {
                            if (msgtype != "0") {
                                global.client.messages.unshift({
                                    json: message,
                                    time: moment().now(),
                                    direction: 0
                                });
                            }
                        }
                    });

                    session.on('err', function(err) {
                        var error = err.message;
                        log.error("ERROR:"+error);
                    });

                    session.on('connect', function() {
                        session.sendLogon(null);
                    });

                    session.on('disconnect', function() {
                        global.client.isconnected = false;
                        log.info("Disconnected!");
                    });

                    session.on('logon', function(msg) {
                        global.client.isconnected = true;
                        res.send(msg);
                    });
                }
            });
        }
    });
});

router.post('/logoff', function(req, res) {
    if (global.client != undefined) {
        global.client.sendLogoff();
        res.send({});
    } else {
        res.status(400).send( { error: 'Client does not exists.' } );
    }
});

router.post('/securitydefinition/request', function(req, res, next) {
    var request = null;

    var security_type = req.body.type;
    var strike_price = req.body.strike_price;
    var maturity_date = req.body.maturity_date;
    var reserved_field = req.body.reserved_field;
    var reference_instrument = req.body.reference_instrument;
    var reference_price = req.body.reference_price;
    var near_month_type = req.body.near_month_type;
    var far_month_type = req.body.far_month_type;
    var near_maturity_date = req.body.near_maturity_date;



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


module.exports = router;
