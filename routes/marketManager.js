var util = require('util');
var net = require('net');
var dict = require('dict');
var FixServer = require('nodefix').FixServer;
var JseServer = require('nodejse').JseServer;
var fs = require('fs');
var moment = require('moment');
var dictPath = require("path").join(__dirname, "dict");
var utils = require("./utils.js");
var MitRuler = require('./ruler/mit/mit.js');
var JseDerivRuler = require('./ruler/jsederiv/jsederiv.js');
var Log = require('log');

module.exports = MarketManager;

/*==================================================*/
/*====================MarketManager====================*/
/*==================================================*/
function MarketManager(market) {
    var self = this;
    var log = new Log('INFO');
    self.market = market;
    self.orders = [];
    self.trades = [];
    self.depth = dict();

    self.orderentry = null;
    self.recovery = null;
    self.dropcopy = null;
    self.posttrade = null;

    self.sessionOptions = dict();

    var startJSEGateway = function(port, config, cb) {
        utils.getDictionary(config.spec, function(err, dictionary) {
            if (err) log.error(err);
            else {
                var accounts = JSON.parse(config.accounts);

                var server = new JseServer(port, dictionary, null, accounts);
                server.createServer(function(session) {
                    if (session) {
                        session.on('outmsg', function(outmsg) {
                            var acct = outmsg.account;
                            var message = outmsg.message;
                            if (message['MsgType'] == "0")
                                log.debug("- OUT\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                            else
                                log.info("- OUT\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                            var options = session.getOptions(acct);
                            if (options != undefined) {
                                self.sessionOptions.set(acct, options);
                            }
                        });

                        session.on('msg', function(msg) {
                            var acct = msg.account;
                            var message = msg.message;
                            if (message['MsgType'] == "0")
                                log.debug("- IN\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                            else
                                log.info("- IN\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                            var options = session.getOptions(acct);
                            if (options != undefined) {
                                self.sessionOptions.set(acct, options);
                            }
                            self.ruler.process('JSE', message['MsgType'], session, msg);
                        });

                        session.on('error', function(err) {
                            var acct = err.account;
                            var error = err.message;
                            log.error("\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nERROR:"+error+"\r\n");
                        });

                        session.on('close', function(data) {
                            var acct = data.account;
                            self.ruler.handleCancelOnDisconnect(acct);
                            log.info("\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nCONNECTION CLOSED!\r\n");
                        });

                        session.on('logon', function(msg) {
                            var acct = msg.account;
                            log.info("\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nLOGON!\r\n");
                            if (self.sessionOptions.has(acct)) {
                                var options = self.sessionOptions.get(acct);
                                if (options != undefined) {
                                    session.modifyBehavior(acct, { 'outgoingSeqNum': options.outgoingSeqNum });
                                }
                            } else {
                                self.sessionOptions.set(acct, null);
                            }
                        });
                        cb(session);
                    }
                });
            }
        })
    }

    var startFixGateway = function(port, config, cb) {
        utils.getDictionary(config.spec, function(err, dictionary) {
            if (err) cb(err, null);
            else {
                var fixversion = config.fixversion;
                var options = JSON.parse(config.options);
                var accounts = JSON.parse(config.accounts);

                var server = new FixServer(port, fixversion, dictionary, options, accounts);
                server.createServer(function(session) {
                    if (session) {
                        session.on('outmsg', function(outmsg) {
                            var acct = outmsg.account;
                            var message = outmsg.message;
                            if (message['35'] == "0")
                                log.debug("- OUT\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                            else
                                log.info("- OUT\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                            var options = session.getOptions(acct);
                            if (options != undefined) {
                                self.sessionOptions.set(acct, options);
                            }
                        });

                        session.on('msg', function(msg) {
                            var acct = msg.account;
                            var message = msg.message;
                            if (message['35'] == "0")
                                log.debug("- IN\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                            else
                                log.info("- IN\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nMESSAGE:"+JSON.stringify(message)+"\r\n");
                            var options = session.getOptions(acct);
                            if (options != undefined) {
                                self.sessionOptions.set(acct, options);
                            }
                            self.ruler.process('FIX', message['35'], session, msg);
                        });

                        session.on('error', function(err) {
                            var acct = err.account;
                            var error = err.message;
                            log.error("\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nERROR:"+error+"\r\n");
                        });

                        session.on('close', function(data) {
                            var acct = data.account;
                            log.info("\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nCONNECTION CLOSED!\r\n");
                        });

                        session.on('logon', function(msg) {
                            var acct = msg.account;
                            log.info("\r\nPORT:"+port+"\r\nACCOUNT:"+acct+"\r\nLOGON!\r\n");
                            if (self.sessionOptions.has(acct)) {
                                var options = self.sessionOptions.get(acct);
                                if (options != undefined) {
                                    session.modifyBehavior(acct, { 'outgoingSeqNum': options.outgoingSeqNum });
                                }
                            } else {
                                self.sessionOptions.set(acct, null);
                            }
                        });
                        cb(session);
                    }
                });
            }
        })
    }

    var startGateway = function(config, type) {
        switch(type) {
            case 'OE':
                startJSEGateway(config.port, config, function(server) {
                    self.orderentry = server;
                    self.ruler.gateways.orderentry = server;
                });
                break;
            case 'RE':
                startJSEGateway(config.recoveryport, config, function(server) {
                    self.recovery = server;
                });
                break;
            case 'DC':
                startFixGateway(config.port, config, function(server) {
                    self.dropcopy = server;
                    self.ruler.gateways.dropcopy = server;
                });
                break;
            case 'PT':
                startFixGateway(config.port, config, function(server) {
                    self.posttrade = server;
                    self.ruler.gateways.posttrade = server;
                });
                break;
        }
    }

    this.start = function(cb){
        switch(self.market.type) {
            case "MIT":
                self.ruler = new MitRuler(self.market, log);
                break;
            case "JSEDERIV":
                self.ruler = new JseDerivRuler(self.market, log);
                break;
        }

        var oe_config = self.market.gateways.orderentry;
        startGateway(oe_config, 'OE')
        startGateway(oe_config, 'RE')
        var dc_config = self.market.gateways.dropcopy;
        startGateway(dc_config, 'DC')
        var pt_config = self.market.gateways.posttrade;
        startGateway(pt_config, 'PT')

        cb();
    }

    this.stop = function(cb) {
        self.orderentry.destroyConnection();
        self.recovery.destroyConnection();
        self.dropcopy.destroyConnection();
        self.posttrade.destroyConnection();

        setTimeout(function(){
            self.orderentry = null;
            self.recovery = null;
            self.dropcopy = null;
            self.posttrade = null;
            self.ruler.clearIntervals(function() {
                self.ruler = null;
            });
            cb();
        }, 1000)

    }

    this.publishNews = function(news, cb) {
        self.ruler.publish_news(self.orderentry, news, function() {
            cb();
        });
    }

}
