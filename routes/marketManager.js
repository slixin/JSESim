var util = require('util');
var net = require('net');
var dict = require('dict');
var FixServer = require('nodefix').FixServer;
var JseServer = require('nodejse');
var fs = require('fs');
var dictPath = require("path").join(__dirname, "dict");
var utils = require("./utils.js");
var MitRuler = require('./ruler/mit.js');
var JseDerivRuler = require('./ruler/jsederiv.js');

module.exports = MarketManager;

/*==================================================*/
/*====================MarketManager====================*/
/*==================================================*/
function MarketManager(market) {
    var self = this;

    self.market = market;
    self.orders = [];
    self.trades = [];
    self.depth = dict();

    self.orderentry = null;
    self.recovery = null;
    self.dropcopy = null;
    self.posttrade = null;

    self.sessionOptions = dict();

    var getDictionary = function(dict_file_name, cb) {
        fs.readFile(dictPath+'/'+dict_file_name+'.json', 'utf8', function (err, data) {
            if (err) cb(err, null);
            else cb(null, JSON.parse(data));
        });
    }

    var startJSEGateway = function(port, config, cb) {
        getDictionary(config.spec, function(err, dictionary) {
            if (err) console.log(err);
            else {
                var accounts = JSON.parse(config.accounts);

                var server = new JseServer(port, dictionary, null, accounts);
                server.createServer(function(session) {
                    if (session) {
                        session.on('outmsg', function(outmsg) {
                            console.log("["+port+"] OUT:" + JSON.stringify(outmsg));
                            var options = session.getOptions(outmsg.account);
                            if (options != undefined) {
                                self.sessionOptions.set(outmsg.account, options);
                            }
                        });

                        session.on('msg', function(msg) {
                            console.log("["+port+"] IN:" +JSON.stringify(msg));
                            var options = session.getOptions(msg.account);
                            if (options != undefined) {
                                self.sessionOptions.set(msg.account, options);
                            }
                            self.ruler.process('JSE', msg.message['MsgType'], session, msg);
                        });

                        session.on('error', function(err) {
                            console.log("["+port+"] ERROR:" + JSON.stringify(err));
                        });

                        session.on('create', function() {
                            console.log("["+port+"] SOCKET CREATED");

                        });

                        session.on('endsession', function(data) {
                            console.log("["+port+"] SESSION ENDED");
                        });

                        session.on('close', function(data) {
                            console.log("["+port+"] CONNECTION CLOSED");
                        });

                        session.on('logon', function(msg) {
                            console.log("["+port+"] LOGON:" + JSON.stringify(msg));
                            var account = msg.account;
                            if (self.sessionOptions.has(account)) {
                                var options = self.sessionOptions.get(account);
                                if (options != undefined) {
                                    session.modifyBehavior(account, { 'outgoingSeqNum': options.outgoingSeqNum });
                                }
                            } else {
                                self.sessionOptions.set(account, null);
                            }
                        });
                        cb(session);
                    }
                });
            }
        })
    }

    var startFixGateway = function(port, config, cb) {
        getDictionary(config.spec, function(err, dictionary) {
            if (err) cb(err, null);
            else {
                var fixversion = config.fixversion;
                var options = JSON.parse(config.options);
                var accounts = JSON.parse(config.accounts);

                var server = new FixServer(port, fixversion, dictionary, options, accounts);
                server.createServer(function(session) {
                    if (session) {
                        session.on('outmsg', function(outmsg) {
                            console.log("["+port+"] OUT:" + JSON.stringify(outmsg));
                            var options = session.getOptions(outmsg.account);
                            if (options != undefined) {
                                self.sessionOptions.set(outmsg.account, options);
                            }
                        });

                        session.on('msg', function(msg) {
                            console.log("["+port+"] IN:" +JSON.stringify(msg));
                            var options = session.getOptions(msg.account);
                            if (options != undefined) {
                                self.sessionOptions.set(msg.account, options);
                            }
                            self.ruler.process('FIX', msg.message['35'], session, msg);
                        });

                        session.on('error', function(err) {
                            console.log("["+port+"] ERROR:" + JSON.stringify(err.message));
                        });

                        session.on('create', function() {
                            console.log("["+port+"] SOCKET CREATED");

                        });

                        session.on('close', function() {
                            console.log("["+port+"] CONNECTION CLOSED");
                        });

                        session.on('logon', function(msg) {
                            console.log("["+port+"] LOGON:" + JSON.stringify(msg));
                            var account = msg.account;
                            if (self.sessionOptions.has(account)) {
                                var options = self.sessionOptions.get(account);
                                if (options != undefined) {
                                    session.modifyBehavior(account, { 'outgoingSeqNum': options.outgoingSeqNum });
                                }
                            } else {
                                self.sessionOptions.set(account, null);
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
                self.ruler = new MitRuler(self.market);
                break;
            case "JSEDERIV":
                self.ruler = new JseDerivRuler(self.market);
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
        self.orderentry = null;
        self.recovery = null;
        self.dropcopy = null;
        self.posttrade = null;
        self.ruler.clearIntervals(function() {
            self.ruler = null;
        });
        cb();
    }

    this.publishNews = function(news, cb) {
        self.ruler.publish_news(self.orderentry, news, function() {
            cb();
        });
    }

}
