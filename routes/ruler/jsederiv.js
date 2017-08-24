var request = require('request');
var utils = require('../utils.js');
var async = require('async');
var dict = require('dict');
var moment = require('moment');
var _ = require('underscore');
var template = require('./jsederiv.json');

module.exports = JseDerivRuler;

function JseDerivRuler(market) {
    var self = this;

    self.market = market;

    self.parties = JSON.parse(market.parties);
    self.instruments = market.instruments;
    self.gateways = {
        orderentry: null,
        posttrade: null,
        dropcopy: null
    }
    self.orders = [];
    self.trades = [];
    self.lastprice = dict();

    // ******************* Monitors ************************
    // Monitor all orders which are going to be created.
    var monitor_create_orders = function() {
        var create_orders = self.orders.filter(function(o) { return o.status == "CREATE" });
        if (create_orders.length > 0) {
            create_orders.forEach(function(order) {
                var message_template = template.native.ack_create;

                if (parseInt(order.data.OrderQuantity) > 999999999) {
                    order.data.RejectCode = "009901" // Invalid value in field
                    rejectOrder(order, function() {});
                    return;
                }

                if (order.data.OrderType == 2 && parseInt(order.data.LimitPrice) < 0.1) {
                    order.data.RejectCode = "009901" // Invalid value in field
                    rejectOrder(order, function() {});
                    return;
                }

                if (order.data.SecurityID == '1003104') {
                    rejectAdmin(order.session, order.account, '009001', 'Unknown order book', order.data.MsgType, order.data.ClientOrderID, function() {});
                    order.status = 'CLOSED';
                    return;
                }

                // Verify some TIF are not supported, and reject
                if (order.data.OrderType == 3 || order.data.OrderType == 4 || order.data.OrderType == 5 || order.data.OrderType == 6) {
                    var invalidTIFForStopOrder = [5, 9, 51,10, 12]; //OPG, GFA, GFX ATC and CPX
                    if (invalidTIFForStopOrder.indexOf(parseInt(order.data.TimeInForce)) >=0 ) {
                        order.data.RejectCode = "001500" //Invalid TIF (unknown)
                        rejectOrder(order, function() {});
                        return;
                    }
                }

                if (order.status == "CREATE") {
                    // Handle Market order as special one
                    switch(order.data.OrderType) {
                        case 1: //Market order
                        case 5: // Market to Limit order
                            processMarketOrder(order);
                            break;
                        default:
                            order.session.sendMsg(message_template, order.account, order.data, function(data) {
                                _updateData(order, data);
                                order.status = "CREATED";
                                _send_dropcopy_message(template.fix.dc_new_order, order, function() {});
                                // If it is a limit order
                                if (order.data.OrderType == 2) {
                                    processLimitOrder(order);
                                }
                            });
                    }
                }
            });
        }
    }

    // Monitor all orders which are going to be amended.
    var monitor_amend_orders = function() {
        var amend_orders = self.orders.filter(function(o) { return o.status == "AMEND" });
        if (amend_orders.length > 0) {
            amend_orders.forEach(function(order) {
                if (parseInt(order.data.OrderQuantity) > 999999999) {
                    order.data.RejectCode = "009901" // Invalid value in field
                    rejectOrder(order, function() {});
                    return;
                }

                if (parseFloat(order.data.LimitPrice) == 0.9999 ) {
                    rejectAdmin(order.session, order.account, '009999', 'System suspended', order.data.MsgType, order.data.ClientOrderID, function() {});
                    order.status = 'CLOSED';
                    return;
                }

                if (order.data.OrderType == 2 && parseFloat(order.data.LimitPrice) < 0.1) {
                    order.data.RejectCode = "009901" // Invalid value in field
                    rejectOrder(order, function() {});
                    return;
                }

                var message_template = template.native.ack_amend;

                order.session.sendMsg(message_template, order.account, order.data, function(data) {
                    _updateData(order, data);
                    order.status = "AMENDED";
                    _send_dropcopy_message(template.fix.dc_amend_order, order, function() {});
                    if (order.data.OrderType == 2 || (order.data.OrderType == 4 && order.data.Container == 1)) {
                        processLimitOrder(order);
                    }
                });
            });
        }
    }

    // Monitor all orders which are going to be cancelled.
    var monitor_cancel_orders = function() {
        var cancel_orders = self.orders.filter(function(o) { return o.status == "CANCEL" });
        if (cancel_orders.length > 0) {
            cancel_orders.forEach(function(order) {
                if (parseFloat(order.data.LimitPrice).toFixed(3) == 9.014 ) {
                    order.data.RejectCode = "009014" // Instrument in Pre-Trading session ER Matching Engine
                    rejectOrder(order, function() {});
                    return;
                }

                var message_template = template.native.ack_cancel;
                _send_dropcopy_message(template.fix.dc_cancel_order, order, function() {});
                order.session.sendMsg(message_template, order.account, order.data, function(data) {
                    _updateData(order, data);
                    order.status = "CLOSED";
                });
            });
        }
    }

    // Monitor GTT / GTD orders
    var monitor_timing_orders = function() {
        var timing_orders = self.orders.filter(function(o) { return o.status != "CLOSED" && (parseInt(o.data.TimeInForce) == 6 || parseInt(o.data.TimeInForce) == 8) && (o.data.OrderStatus == 0 || o.data.OrderStatus == 1)});
        if (timing_orders.length > 0) {
            timing_orders.forEach(function(order) {
                var expiretime = order.data.ExpireTime.substr(0,4)+'-'+order.data.ExpireTime.substr(4,2)+'-'+order.data.ExpireTime.substr(6,2)+' '+order.data.ExpireTime.substr(9,order.data.ExpireTime.length - 9);
                var utc_expiretime = moment.utc(expiretime);
                var utc_now = moment.utc();
                var time_diff = utc_expiretime.diff(utc_now, 'minutes');
                if (time_diff == 0) { // Expired
                    expireOrder(order, function(){});
                }
            });
        }
    }

    // Monitor Stop / Stop Limit / Market If Touched orders
    var monitor_stop_orders = function() {
        var stop_orders = self.orders.filter(function(o) { return parseInt(o.data.Container) == 6 && o.data.OrderStatus == 0 && parseFloat(o.data.TrailingOffset) == 0 });
        stop_orders.forEach(function(order) {
            triggerStopOrder(order, function(triggered_order){
                if (triggered_order != undefined) {
                    switch(triggered_order.data.OrderType) {
                        case 3: //  When it is a stop / market if touched order, and triggered, process it as market order.
                        case 6:
                            processMarketOrder(triggered_order);
                            break;
                        case 4: // When it is a stop limit order, and triggered, process it as limit order.
                            processLimitOrder(triggered_order);
                            break;
                    }
                }
            });
        })
    }

    // Monitor Trailing Stop / Trailing Stop Limit orders
    var monitor_trailing_orders = function() {
        var trailing_orders = self.orders.filter(function(o) { return parseInt(o.data.Container) == 6 && o.data.OrderStatus == 0 && parseFloat(o.data.TrailingOffset) > 0 });
        trailing_orders.forEach(function(order) {
            triggerTrailingOrder(order, function(triggered_order){
                if (triggered_order != undefined) {
                    switch(triggered_order.data.OrderType) {
                        case 3: //  When it is a stop order, and triggered, process it as market order.
                            processMarketOrder(triggered_order);
                            break;
                        case 4: // When it is a stop limit order, and triggered, process it as limit order.
                            processLimitOrder(triggered_order);
                            break;
                    }
                }
            });
        })
    }

    var timer_create_orders = setInterval(monitor_create_orders, 1000);
    var timer_amend_orders = setInterval(monitor_amend_orders, 1000);
    var timer_cancel_orders = setInterval(monitor_cancel_orders, 1000);
    var timer_timing_orders = setInterval(monitor_timing_orders, 1000);
    var timer_stop_orders = setInterval(monitor_stop_orders, 1000);
    var timer_trailing_orders = setInterval(monitor_trailing_orders, 1000);

    // *****************************************************************
    var processMarketOrder = function(order) {
        var match_order_side = parseInt(order.data.Side) == 1 ? 2: 1;
        var matchingOrders = self.orders.filter(function(o) {
            return parseInt(o.data.Side) == match_order_side &&
                    o.data.SecurityID == order.data.SecurityID &&
                    o.data.ClientOrderID != order.data.ClientOrderID &&
                    (parseInt(o.data.OrderStatus) == 0 || parseInt(o.data.OrderStatus) == 1) &&
                    (parseInt(o.data.Container) == 1 || parseInt(o.data.Container) == 21)
        }).sort(function(a,b) {
            var ret = null;
            if (match_order_side == 1) {
                ret = a.data.LimitPrice < b.data.LimitPrice ? 1 : b.data.LimitPrice < a.data.LimitPrice ? -1 : 0;
            } else {
                ret = a.data.LimitPrice > b.data.LimitPrice ? 1 : b.data.LimitPrice > a.data.LimitPrice ? -1 : 0;
            }

            return ret;
        });
        if (matchingOrders.length > 0) {
            trading(order, matchingOrders, function(leavesqty) {
                if (leavesqty > 0) {
                    switch(order.data.OrderType) {
                        case 1: // Market order
                        case 3: // Stop order
                            expireOrder(order, function(){});
                            break;
                        case 5: // Convert Market to Limit Order
                            restateOrder(order, function(){});
                            break;
                    }
                }
            });
        } else {
            if (order.data.OrderID == undefined) {
                order.data.OrderID = "O"+utils.randomString(null, 7);
            }
            switch(order.data.OrderType) {
                case 1: // Market Order
                case 3: // Stop order
                    expireOrder(order, function(){});
                    break;
                case 5: // Convert Market to Limit Order
                    if (order.data.TimeInForce == 3 || order.data.TimeInForce == 4) { // When it is IOC or FOK, expire
                        expireOrder(order, function(){});
                    } else {
                        order.data.LeavesQuantity = order.data.OrderQuantity;
                        restateOrder(order, function(){});
                    }
                    break;
            }
        }
    }

    var processLimitOrder = function(order) {
        var match_order_side = parseInt(order.data.Side) == 1 ? 2: 1;
        var matchingOrders = null;

        if (match_order_side == 1) {
            matchingOrders = self.orders.filter(function(o) {
                return parseInt(o.data.Side) == match_order_side &&
                o.data.SecurityID == order.data.SecurityID &&
                o.data.ClientOrderID != order.data.ClientOrderID &&
                o.data.LimitPrice >= order.data.LimitPrice &&
                (parseInt(o.data.OrderStatus) == 0 || parseInt(o.data.OrderStatus) == 1) &&
                (parseInt(o.data.Container) == 1 || parseInt(o.data.Container) == 21)
            }).sort(function(a,b) {
                var a_price = a.data.LimitPrice;
                var b_price = b.data.LimitPrice;
                var ret = null;
                if (match_order_side == 1) {
                    ret = a_price < b_price ? 1 : b_price < a_price ? -1 : 0;
                } else {
                    ret = a_price > b_price ? 1 : b_price > a_price ? -1 : 0;
                }
                return ret;
            });
        } else {
            matchingOrders = self.orders.filter(function(o) {
                return parseInt(o.data.Side) == match_order_side &&
                o.data.SecurityID == order.data.SecurityID &&
                o.data.ClientOrderID != order.data.ClientOrderID &&
                o.data.LimitPrice <= order.data.LimitPrice &&
                (parseInt(o.data.OrderStatus) == 0 || parseInt(o.data.OrderStatus) == 1) &&
                (parseInt(o.data.Container) == 1 || parseInt(o.data.Container) == 21)
            }).sort(function(a,b) {
                var a_price = a.data.LimitPrice;
                var b_price = b.data.LimitPrice;
                var ret = null;
                if (match_order_side == 1) {
                    ret = a_price < b_price ? 1 : b_price < a_price ? -1 : 0;
                } else {
                    ret = a_price > b_price ? 1 : b_price > a_price ? -1 : 0;
                }
                return ret;
            });
        }

        if (order.data.TimeInForce == 4) {//FOK
            matchingOrders = matchingOrders.filter(function(o) {
                return o.data.OrderQuantity >= order.data.OrderQuantity;
            });
        }

        if (matchingOrders.length > 0) {
            trading(order, matchingOrders, function(leavesqty) {
                if (leavesqty > 0) {
                    if (order.data.TimeInForce == 3) { //IOC
                        expireOrder(order, function(){});
                    }
                }
            });
        } else {
            if (order.data.TimeInForce == 3 || order.data.TimeInForce == 4) { //FOK or IOC
                expireOrder(order, function() {});
            }
        }
    }

    var rejectAdmin = function(session, account, code, reason, type, crossID, cb) {
        var message_template = template.native.ack_admin_reject;

        var data = {
            "RejectCode": code,
            "RejectReason": reason,
            "MessageType": type,
            "ClientOrderID": crossID
        }

        session.sendMsg(message_template, account, data, function(data) {
            cb();
        });
    }

    var rejectOrder = function(order, cb) {
        var session = order.session;
        var message_template = template.native.ack_reject;
        session.sendMsg(message_template, order.account, order.data, function(data) {
            _updateData(order, data);
            order.status = "CLOSED";
            cb();
        });
    }

    var expireOrder = function(order, cb) {
        var session = order.session;
        var message_template = template.native.ack_expire;

        session.sendMsg(message_template, order.account, order.data, function(data) {
            _updateData(order, data);
            order.status = "CLOSED";
            _send_dropcopy_message(template.fix.dc_expire_order, order, function() {});
            cb();
        });
    }

    var restateOrder = function(order, cb) {
        var session = order.session;
        var message_template = template.native.ack_restate;

        var dynamic_reference_price = 0;

        if (self.lastprice.has(order.data.SecurityID.toString())) {
            dynamic_reference_price = parseFloat(self.lastprice.get(order.data.SecurityID.toString()));
        }
        order.data.OrderType = 2;
        order.data.LimitPrice = parseFloat(order.data.ExecutedPrice) == 0 ? dynamic_reference_price : order.data.ExecutedPrice;

        session.sendMsg(message_template, order.account, order.data, function(data) {
            _updateData(order, data);
            order.status="RESTATE";
            _send_dropcopy_message(template.fix.dc_restate_order, order, function() {});
            cb();
        });
    }

    var triggerStopOrder = function(order, cb) {
        var shouldTrigger = false;
        var message_template = template.native.ack_trigger;
        if (self.lastprice.has(order.data.SecurityID.toString())) {
            var marketPrice = self.lastprice.get(order.data.SecurityID.toString());
            if (marketPrice > 0) {
                if (order.data.Side == 1) { // Buy order
                    if (order.data.OrderType == 3 || order.data.OrderType == 4) { // Is Stop / Stop Limit order
                        if (parseFloat(order.data.StopPrice) <= parseFloat(marketPrice)) shouldTrigger = true;
                    } else { // Market If touched order
                        if (parseFloat(order.data.StopPrice) >= parseFloat(marketPrice)) shouldTrigger = true;
                    }
                } else { // Sell order
                    if (order.data.OrderType == 3 || order.data.OrderType == 4) { // Is Stop / Stop Limit order
                        if (parseFloat(order.data.StopPrice) >= parseFloat(marketPrice)) shouldTrigger = true;
                    } else { // Market If touched order
                        if (parseFloat(order.data.StopPrice) <= parseFloat(marketPrice)) shouldTrigger = true;
                    }
                }
            }
        }
        if (shouldTrigger) {
            order.session.sendMsg(message_template, order.account, order.data, function(data) {
                _updateData(order, data);
                order.status = "TRIGGERED";
                _send_dropcopy_message(template.fix.dc_trigger_order, order, function() {});
                cb(order);
            });
        } else {
            cb(null);
        }
    }

    var triggerTrailingOrder = function(order, cb) {
        var shouldTrigger = false;
        var message_template = template.native.ack_trigger;
        if (self.lastprice.has(order.data.SecurityID.toString())) {
            var marketPrice = self.lastprice.get(order.data.SecurityID.toString());
            var trailingOffset = order.data.TrailingOffset;
            if (marketPrice > 0) {
                if (order.data.Side == 1) { // Buy order
                    if (order.data.LastMarketPrice == undefined) {
                        order.data.StopPrice = parseFloat(marketPrice) + parseFloat(trailingOffset);
                    } else {
                        if (marketPrice < order.data.LastMarketPrice) { // Market price drops
                            order.data.StopPrice = parseFloat(marketPrice) + parseFloat(trailingOffset);
                        }
                    }
                    order.data.LastMarketPrice = marketPrice;
                    if (parseFloat(order.data.StopPrice) <= parseFloat(marketPrice)) shouldTrigger = true;
                } else { // Sell order
                    if (order.data.LastMarketPrice == undefined) {
                        order.data.StopPrice = parseFloat(marketPrice) - parseFloat(trailingOffset);
                    } else {
                        if (marketPrice > order.data.LastMarketPrice) { // Market price rises
                            order.data.StopPrice = parseFloat(marketPrice) - parseFloat(trailingOffset);
                        }
                    }
                    order.data.LastMarketPrice = marketPrice;
                    if (parseFloat(order.data.StopPrice) >= parseFloat(marketPrice)) shouldTrigger = true;
                }
            }
        }
        if (shouldTrigger) {
            order.session.sendMsg(message_template, order.account, order.data, function(data) {
                _updateData(order, data);
                order.status = "TRIGGERED";
                _send_dropcopy_message(template.fix.dc_trigger_order, order, function() {});
                cb(order);
            });
        } else {
            cb(null);
        }
    }

    var trading = function(order, matchingOrders, cb) {
        var leavesqty = parseInt(order.data.OrderQuantity)
        var i = 0;
        async.whilst(
            function () { return i < matchingOrders.length && leavesqty > 0; },
            function (next) {
                var match_order = matchingOrders[i];
                var match_order_leavesqty = parseInt(match_order.data.LeavesQuantity);
                var tradeId = "T"+utils.randomString(null, 8);
                var tradeRptId = "L"+utils.randomString(null, 8);
                var tradeLinkId = "Z"+utils.randomString(null, 8);
                var isTradeable = true;

                // When the match order is a FillOrKill order and the leaves quantity does not fully match, it cannot be traded.
                if (parseInt(match_order.data.TimeInForce) == 4) { // FOK
                    if (parseInt(match_order_leavesqty) != parseInt(leavesqty)) {
                        isTradeable = false;
                    }
                }

                if (!isTradeable){
                    i++;
                    next();
                } else {
                    // Match order volume == order volume, order will be fully traded, match order will be fully traded.
                    if (match_order_leavesqty == leavesqty) {
                        var execprice = match_order.data.LimitPrice;
                        order.data.ExecutedPrice = execprice;
                        order.data.ExecutedQuantity = match_order_leavesqty;
                        order.data.LeavesQuantity = 0;
                        order.data.CumQuantity = parseInt(order.data.CumQuantity == undefined ? 0 : order.data.CumQuantity) + parseInt(leavesqty);
                        order.data.TradeID = tradeId;
                        order.data.TradeReportID = tradeRptId;
                        order.data.TradeLinkID = tradeLinkId;

                        if (order.data.OrderID == undefined) {
                            order.data.OrderID = "O"+utils.randomString(null, 7);
                        }

                        match_order.data.ExecutedPrice = execprice;
                        match_order.data.ExecutedQuantity = match_order_leavesqty;
                        match_order.data.LeavesQuantity = 0;
                        match_order.data.CumQuantity = parseInt(match_order.data.CumQuantity == undefined ? 0 : match_order.data.CumQuantity) + parseInt(leavesqty);;
                        match_order.data.TradeID = tradeId;
                        match_order.data.TradeReportID = tradeRptId;
                        match_order.data.TradeLinkID = tradeLinkId;

                        leavesqty = 0;

                        self.lastprice.set(order.data.SecurityID.toString(), execprice);

                        _trading_fully(order, function() {
                            _trading_fully(match_order, function() {
                                i++;
                                next();
                            })
                        })
                    } else if (match_order_leavesqty > leavesqty){ // Matching order volume > order volume, order will be fully traded, matching order will be partially traded.
                        var execprice = match_order.data.LimitPrice;
                        order.data.ExecutedPrice = execprice;
                        order.data.ExecutedQuantity = leavesqty;
                        order.data.LeavesQuantity = 0;
                        order.data.CumQuantity = parseInt(order.data.CumQuantity == undefined ? 0 : order.data.CumQuantity) + parseInt(leavesqty);
                        order.data.TradeID = tradeId;
                        order.data.TradeReportID = tradeRptId;
                        order.data.TradeLinkID = tradeLinkId;
                        if (order.data.OrderID == undefined) {
                            order.data.OrderID = "O"+utils.randomString(null, 7);
                        }

                        match_order.data.ExecutedPrice = execprice;
                        match_order.data.ExecutedQuantity = leavesqty;
                        match_order.data.LeavesQuantity = parseInt(match_order_leavesqty) - parseInt(leavesqty);
                        match_order.data.CumQuantity = parseInt(match_order.data.CumQuantity == undefined ? 0 : match_order.data.CumQuantity) + parseInt(leavesqty);
                        match_order.data.TradeID = tradeId;
                        match_order.data.TradeReportID = tradeRptId;
                        match_order.data.TradeLinkID = tradeLinkId;

                        leavesqty = 0;

                        self.lastprice.set(order.data.SecurityID.toString(), execprice);

                        _trading_fully(order, function() {
                            _trading_partially(match_order, function() {
                                i++;
                                next();
                            })
                        });
                    } else { // Matching order volume < order volume, order will be partially traded, matching order will be fully traded.
                        var execprice = match_order.data.LimitPrice;
                        order.data.ExecutedPrice = execprice;
                        order.data.ExecutedQuantity = match_order_leavesqty;
                        order.data.LeavesQuantity = parseInt(leavesqty) - parseInt(match_order_leavesqty);
                        order.data.CumQuantity = parseInt(order.data.CumQuantity == undefined ? 0 : order.data.CumQuantity) + parseInt(match_order_leavesqty);
                        order.data.TradeID = tradeId;
                        order.data.TradeReportID = tradeRptId;
                        order.data.TradeLinkID = tradeLinkId;
                        if (order.data.OrderID == undefined) {
                            order.data.OrderID = "O"+utils.randomString(null, 7);
                        }

                        match_order.data.ExecutedPrice = execprice;
                        match_order.data.ExecutedQuantity = match_order_leavesqty;
                        match_order.data.LeavesQuantity = 0;
                        match_order.data.CumQuantity = parseInt(match_order.data.CumQuantity == undefined ? 0 : match_order.data.CumQuantity) + parseInt(match_order_leavesqty);
                        match_order.data.TradeID = tradeId;
                        match_order.data.TradeReportID = tradeRptId;
                        match_order.data.TradeLinkID = tradeLinkId;
                        leavesqty = leavesqty - match_order_leavesqty;

                        self.lastprice.set(order.data.SecurityID.toString(), execprice);

                        _trading_partially(order, function() {
                            _trading_fully(match_order, function() {
                                i++;
                                next();
                            })
                        });
                    }
                }

            },
            function (err) {
                if (err) console.log(err);
                cb(leavesqty);
            }
        );
    }

    // *************** On Book ****************
    var tradeCancel = function(order, cb) {
        var session = order.session;
        var message_template = template.native.ack_trade_cancel;
        order.data.TradeExecutionID = order.data.ExecutionID;
        session.sendMsg(message_template, order.account, order.data, function(data) {
            _updateData(order, data);
            order.status = "CANCELED";
            cb(order);
        });
    }

    var _ack_onbook_trade_cancel = function(order, cb) {
        var onbook_party = _build_onbook_pt_party(order);
        var message_template = _.extend({}, template.fix.pt_onbook_ack_cancel_trade, onbook_party);
        var username = order.trade.data['49'];
        var sessions = _find_pt_sessions(order.broker).filter(function(o) { return o.account == username });
        _send_posttrade_message(sessions, message_template, order.trade.data, 'TRADE_CANCELED', function(tradeId) {
            cb(order);
        });
    }

    var _confirm_onbook_trade_cancel = function(order) {
        var onbook_party = _build_onbook_pt_party(order);
        var message_template = _.extend({}, template.fix.pt_onbook_confirm_cancel_trade, onbook_party);

        message_template['571'] = 'L'+utils.randomString('0123456789', 9);
        message_template['572'] = order.data.TradeReportID;
        message_template['381'] = (parseInt(order.data.ExecutedQuantity) * parseFloat(order.data.ExecutedPrice)).toString();

        var sessions = _find_pt_sessions(order.broker);
        _send_posttrade_message(sessions, message_template, order.trade.data, 'CONFIM_TRADE_CANCELED', function(tradeId) {});
    }

    var cancelOnBookTrade = function(order, cb) {
        _ack_onbook_trade_cancel(order, function(order) {
            tradeCancel(order, function(ord) {
                _confirm_onbook_trade_cancel(ord);
                _send_dropcopy_message(template.fix.dc_trade_cancel, ord);
                if (order.data.LeavesQuantity == 0) {
                    order.status = "CANCEL";
                }
            });
        });
    }
    // ***************************************

    // **************** TCR methods ******************
    var _build_single_party_tcr_parties = function(exec_side, exec_broker, oppo_side, oppo_broker) {
        var noPartyIDs = { "552" : [] };
        var exec = null;
        var oppo = null;

        var exec_party = self.parties.filter(function(o) { return o.trader == exec_broker });
        var oppo_party = self.parties.filter(function(o) { return o.trader == oppo_broker });

        exec = {
            "54": exec_side,
            "453": [
                { "448": exec_party[0].trader,  "447": "D", "452": 1 },
                { "448": exec_party[0].tradergroup,  "447": "D", "452": 53 },
                { "448": exec_party[0].firm,  "447": "D", "452": 76 },
            ]
        }

        oppo = {
            "54": oppo_side,
            "453": [
                { "448": oppo_party[0].trader,  "447": "D", "452": 17 },
                { "448": oppo_party[0].tradergroup,  "447": "D", "452": 37 },
                { "448": oppo_party[0].firm,  "447": "D", "452": 100 },
            ]
        }

        noPartyIDs["552"].push(exec);
        noPartyIDs["552"].push(oppo);

        return noPartyIDs;
    }

    var _build_dual_party_tcr_parties = function(exec_side, exec_broker, oppo_side, oppo_broker, type) {
        var noPartyIDs = { "552" : [] };
        var exec = null;
        var oppo = null;

        var exec_party = self.parties.filter(function(o) { return o.trader == exec_broker });
        var oppo_party = self.parties.filter(function(o) { return o.trader == oppo_broker });

        switch(type) {
            case 1: // Notify
                exec = {
                    "54": exec_side,
                    "453": [
                        { "448": exec_party[0].trader,  "447": "D", "452": 17 }
                    ]
                }

                oppo = {
                    "54": oppo_side,
                    "453": [
                        { "448": oppo_party[0].trader,  "447": "D", "452": 1 }
                    ]
                }
                noPartyIDs["552"].push(oppo);
                noPartyIDs["552"].push(exec);

                break;
            default:
                exec = {
                    "54": exec_side,
                    "453": [
                        { "448": exec_party[0].trader,  "447": "D", "452": 1 },
                        { "448": exec_party[0].tradergroup,  "447": "D", "452": 53 },
                        { "448": exec_party[0].firm,  "447": "D", "452": 76 },
                    ]
                }

                oppo = {
                    "54": oppo_side,
                    "453": [
                        { "448": oppo_party[0].trader,  "447": "D", "452": 17 }
                    ]
                }
                noPartyIDs["552"].push(exec);
                noPartyIDs["552"].push(oppo);

                break;
        }


        return noPartyIDs;
    }

    // PartyRole, 1 - Exec, 17 - Counter
    var _find_party = function(sides, partyrole) {
        var party_side = null;
        sides.forEach(function(side) {
            var party_info = side['453'];
            var f_party = party_info.filter(function(o) { return o['452'] == partyrole});
            if (f_party.length > 0) {
                party_side = side;
                return;
            }
        })

        return party_side;
    }

    var _find_pt_sessions = function(broker) {
        var sessions = [];

        var accounts = self.gateways.posttrade.accounts.filter(function(o) { return o.brokerid == broker });
        if (accounts.length > 0) {
            accounts.forEach(function(acct) {
                var username = acct.targetID;
                if (self.gateways.posttrade.clients.has(username)) {
                    sessions.push({ session: self.gateways.posttrade, account: username });
                }
            });
        }
        return sessions;
    }

    var _ack_tcr = function(tcr, template, status, cb) {
        var parties = null;
        var sides = tcr["552"];
        var tcr_type = tcr["1123"];

        var exec_party = _find_party(sides, 1);
        var counter_party = _find_party(sides, 17);

        var exec_broker = exec_party['453'][0]['448'];

        if (tcr_type == 1) { // Single party TCR
            parties = _build_single_party_tcr_parties(exec_party["54"], exec_party["453"][0]["448"], counter_party["54"], counter_party["453"][0]["448"]);
        } else { // Dual Party TCR
            parties = _build_dual_party_tcr_parties(exec_party["54"], exec_party["453"][0]["448"], counter_party["54"], counter_party["453"][0]["448"], 0);
        }

        var exec_sessions = _find_pt_sessions(exec_broker).filter(function(o) { return o.account == tcr['49']});
        var ack_msg = _.extend({}, template, parties);
        _send_posttrade_message(exec_sessions, ack_msg, tcr, status, function(tradeId) {
            var f_trades = self.trades.filter(function(o) { return o.tradeId == tradeId});
            if (f_trades.length > 0) {
                cb(f_trades[0]);
            } else {
                cb(null);
            }
        });
    }

    var _confirm_tcr = function(trade, template, status) {
        var tcr = trade.data;
        var sides = tcr["552"];
        var tcr_type = tcr["1123"];

        var exec_parties = null;
        var counter_parties = null;

        var exec_party = _find_party(sides, 1);
        var counter_party = _find_party(sides, 17);

        var exec_broker = exec_party['453'][0]['448'];
        var counter_broker = counter_party['453'][0]['448'];

        if (tcr_type == 1) { // Single party TCR
            exec_parties = _build_single_party_tcr_parties(exec_party["54"], exec_party["453"][0]["448"], counter_party["54"], counter_party["453"][0]["448"]);
            counter_parties = _build_single_party_tcr_parties(counter_party["54"], counter_party["453"][0]["448"], exec_party["54"], exec_party["453"][0]["448"]);
        } else { // Dual Party TCR
            exec_parties = _build_dual_party_tcr_parties(exec_party["54"], exec_party["453"][0]["448"], counter_party["54"], counter_party["453"][0]["448"], 0);
            counter_parties = _build_dual_party_tcr_parties(counter_party["54"], counter_party["453"][0]["448"], exec_party["54"], exec_party["453"][0]["448"], 0);
        }
        var exec_sessions = _find_pt_sessions(exec_broker);
        var exec_confirm_msg = _.extend({}, template, exec_parties);
        exec_confirm_msg['487'] = 2;
        _send_posttrade_message(exec_sessions, exec_confirm_msg, tcr, status, function(tradeId) {});

        var counter_sessions = _find_pt_sessions(counter_broker);
        var counter_confirm_msg = _.extend({}, template, counter_parties);
        counter_confirm_msg['487'] = tcr_type == 1 ? 1 : 2;
        _send_posttrade_message(counter_sessions, counter_confirm_msg, tcr, status, function(tradeId) {});
    }

    var _notify_tcr = function(trade, template, status) {
        var tcr = trade.data;
        var sides = tcr['552'];

        var exec_party = _find_party(sides, 1);
        var counter_party = _find_party(sides, 17);

        var counter_broker = counter_party['453'][0]['448'];

        var counter_sessions = _find_pt_sessions(counter_broker);
        var parties = _build_dual_party_tcr_parties(exec_party["54"], exec_party["453"][0]["448"], counter_party["54"], counter_party["453"][0]["448"], 1);
        var notify_msg = _.extend({}, template, parties);

        _send_posttrade_message(counter_sessions, notify_msg, tcr, status, function(tradeId) {});
    }

    var _confirm_trade_tcr = function(trade) {
        _confirm_tcr(trade, template.fix.pt_confirm_new_tcr, 'TRADED');
    }
    var _confirm_cancel_tcr = function(trade) {
        _confirm_tcr(trade, template.fix.pt_confirm_cancel_tcr, 'CANCELED');
    }
    var _confirm_decline_tcr = function(trade) {
        _confirm_tcr(trade, template.fix.pt_confirm_decline_tcr, 'DECLINED');
    }

    var _ack_reject_tcr = function(tcr, reject_reason, reject_text) {
        var msg_template = template.fix.pt_ack_reject_tcr;
        msg_template['751'] = reject_reason;
        msg_template['58'] = reject_text;
        _ack_tcr(tcr, msg_template, "REJECTED", function(trade) {});
    }
    var _ack_new_tcr = function(tcr, status, cb) {
        tcr['1003'] = "M"+utils.randomString('0123456789', 8);
        tcr['820'] = "Z"+utils.randomString('0123456789', 8);
        _ack_tcr(tcr, template.fix.pt_ack_new_tcr, status, function(trade) {
            cb(trade);
        });
    }
    var _ack_accept_decline_tcr = function(tcr, status, cb) {
        _ack_tcr(tcr, template.fix.pt_ack_accept_decline_tcr, status, function(trade) {
            cb(trade);
        });
    }
    var _ack_cancel_tcr_response = function(tcr, status, cb) {
        _ack_tcr(tcr, template.fix.pt_ack_cancel_tcr, status, function(trade) {
            cb(trade);
        });
    }
    var _ack_withdraw_tcr = function(tcr, status, cb) {
        _ack_tcr(tcr, template.fix.pt_ack_withdraw_tcr, status, function(trade) {
            cb(trade);
        });
    }

    var _notify_new_tcr = function(trade) {
        _notify_tcr(trade, template.fix.pt_tcr_new_notify, "NOTIFIED_CREATE");
    }
    var _notify_cancel_tcr = function(trade) {
        _notify_tcr(trade, template.fix.pt_tcr_cancel_notify, "NOTIFIED_CANCEL");
    }
    var _notify_reject_cancel_tcr = function(trade) {
        _notify_tcr(trade, template.fix.pt_tcr_cancel_reject_notify, "NOTIFIED_REJECT_CANCEL");
    }
    var _notify_withdraw_tcr = function(trade) {
        _notify_tcr(trade, template.fix.pt_tcr_withdraw_notify, "NOTIFIED_WITHDRAW");
    }
    var _notify_withdraw_cancel_tcr = function(trade) {
        _notify_tcr(trade, template.fix.pt_tcr_withdraw_cancel_notify, "NOTIFIED_CANCEL_WITHDRAW");
    }

    // ************ Single Party ***************
    var newSinglePartyTCR = function(tcr, cb) {
        if (parseInt(tcr["31"]) == 102) {
            var origTradeTime = moment().subtract(1, 'days').format("YYYYMMDD-HH:mm:ss");
            tcr["122"] = origTradeTime;
        }

        _ack_new_tcr(tcr, 'ACK_CREATE', function(trade) {
            _confirm_trade_tcr(trade);
        });
    }

    var cancelSinglePartyTCR = function(tcr, cb) {
        _ack_cancel_tcr_response(tcr.data, 'ACK_CANCEL', function(trade) {
            _confirm_cancel_tcr(trade);
        });
    }
    // *********************************************

    // ************* Dual Party ********************
    // ---------------- Reject ------------------
    var rejectTCR = function(tcr, reason, text) {
        _ack_reject_tcr(tcr.data, reason, text);
    }
    //--------------------------------------------
    // ---------------- New ---------------------
    var newDualPartyTCR = function(tcr, cb) {
        if (parseInt(tcr["31"]) == 102) {
            var origTradeTime = moment().subtract(1, 'days').format("YYYYMMDD-HH:mm:ss");
            tcr["122"] = origTradeTime;
        }

        _ack_new_tcr(tcr, 'ACK_CREATE', function(trade) {
            _notify_new_tcr(trade)
        });
    }

    var acceptDualPartyTCR = function(tcr, cb) {
        _ack_accept_decline_tcr(tcr.data, 'ACK_ACCEPT', function(trade) {
            _confirm_trade_tcr(trade)
        });
    }

    var declineDualPartyTCR = function(tcr, cb) {
        if (tcr.status == "NOTIFIED_CANCEL") {
            _ack_cancel_tcr_response(tcr.data, 'ACK_CANCEL_REJECTED', function(trade) {
                _notify_reject_cancel_tcr(trade);
            });
        } else {
            _ack_accept_decline_tcr(tcr.data, 'ACK_DECLINE', function(trade) {
                _confirm_decline_tcr(trade);
            });
        }
    }
    // ---------------------------------------------

    // -------------- Cancel ------------------------
    var cancelDualPartyTCR = function(tcr, cb) {
        if (tcr.status == "ACK_WITHDRAW") {
            _ack_reject_tcr(tcr.data, '7050', 'Cancellation process terminated');
        } else {
            _ack_cancel_tcr_response(tcr.data, 'ACK_CANCEL', function(trade) {
                _notify_cancel_tcr(trade);
            });
        }

    }

    var acceptCancelDualPartyTCR = function(tcr, cb) {
        _ack_cancel_tcr_response(tcr.data, 'ACK_CANCEL_ACCEPTED', function(trade) {
            _confirm_cancel_tcr(trade);
        });
    }

    var rejectCancelDualPartyTCR = function(tcr, cb) {
        _ack_cancel_tcr_response(tcr.data, 'ACK_CANCEL_REJECTED', function(trade) {
            _notify_reject_cancel_tcr(trade);
        });
    }
    // ----------------------------------------------

    // ------------------ Withdraw ------------------
    var withdrawDualPartyTCR = function(tcr, cb) {
        if (tcr.status == "TRADED" || tcr.status == "DECLINED") {
            _ack_reject_tcr(tcr.data, '7060', 'Request already accepted/declined');
        } else {
            tcr.data['573'] = 1;
            _ack_withdraw_tcr(tcr.data, 'ACK_WITHDRAW', function(trade) {
                _notify_withdraw_tcr(trade);
            });
        }

    }

    var withdrawCancelDualPartyTCR = function(tcr, cb) {
        if (tcr.status == "NOTIFIED_REJECT_CANCEL") {
            _ack_reject_tcr(tcr.data, '7060', 'Request already accepted/declined');
        } else {
            tcr.data['573'] = 0;
            _ack_withdraw_tcr(tcr.data, 'ACK_WITHDRAW_CANCEL', function(trade) {
                _notify_withdraw_cancel_tcr(trade);
            });
        }
    }
    // ----------------------------------------------
    // ************************************************************************

    // ******************************* Common *******************************
    var _findOrderById = function(id) {
        var results = self.orders.filter(function(o) { return o.data.OrderID == id});
        if (results.length == 1) {
            return results[0];
        } else {
            return null;
        }
    }

    var _updateData = function(obj, message) {
        for(key in message) {
            var ignore = false;
            if (message.hasOwnProperty(key)) {
                if (key == 'ExecutionInstruction') {
                    if ('ExecutionInstruction' in obj.data) {
                        ignore = true;
                    }
                }

                if (key == 'StopPrice') {
                    if (parseFloat(obj.data.TrailingOffset) > 0) { // when it is a trailing stop order
                        ignore = true;
                    }
                }


                if(!ignore) {
                    obj.data[key] = message[key];
                }
            }
        }
    }

    var _build_onbook_pt_party = function(order) {
        var noSide = {
            "54": order.data.Side,
            "1427": order.data.ExecutionID,
            "1444": order.data.Side,
            "1115": "1",
            "37": order.data.OrderID,
            "11": order.data.ClientOrderID,
            "528": "A",
            "1": order.data.Account
        }
        var noPartyIDs = _build_onbook_parties(order.broker);
        var party = {
            "552": [
                _.extend({}, noSide, noPartyIDs)
            ]
        }
        return party;
    }

    var _trading_fully = function(order, cb) {
        var session = order.session;
        var account = order.account;
        var broker = order.broker;
        var order_data = order.data;

        session.sendMsg(template.native.ack_fully_trade, account, order_data, function(data) {
            _updateData(order, data);
            order.status = "TRADED";

            var pt_sessions = _find_pt_sessions(broker);
            var onbook_party = _build_onbook_pt_party(order);
            var message_template = _.extend({}, template.fix.pt_onbook_trade, onbook_party);
            _send_posttrade_message(pt_sessions, message_template, order.data, 'TRADED', function(tradeId) {
                var f_trades = self.trades.filter(function(o) { return o.tradeId == tradeId});
                if (f_trades.length > 0) {
                    order.trade = f_trades[0];
                }
            });
            _send_dropcopy_message(template.fix.dc_onbook_trade, order);
            cb();
        });
    }

    var _trading_partially = function(order, cb) {
        var session = order.session;
        var account = order.account;
        var broker = order.broker;
        var order_data = order.data;
        session.sendMsg(template.native.ack_partially_trade, account, order_data, function(data) {
            _updateData(order, data);
            order.status = "TRADED";

            var pt_sessions = _find_pt_sessions(broker);
            var onbook_party = _build_onbook_pt_party(order);
            var message_template = _.extend({}, template.fix.pt_onbook_trade, onbook_party);
            _send_posttrade_message(pt_sessions, message_template, order.data, 'TRADED', function(tradeId) {
                var f_trades = self.trades.filter(function(o) { return o.tradeId == tradeId});
                if (f_trades.length > 0) {
                    order.trade = f_trades[0];
                }
            });
            _send_dropcopy_message(template.fix.dc_onbook_trade, order);
            cb();
        });
    }

    var _send_dropcopy_message = function(message_template, order) {
        var broker = order.broker;
        var parties = _build_onbook_parties(broker);

        if (parties != undefined) {
            message_template = _.extend({}, message_template, parties);
            var accounts = self.gateways.dropcopy.accounts.filter(function(o) { return o.brokerid == broker });
            if (accounts.length > 0) {
                accounts.forEach(function(acct) {
                    var username = acct.targetID;
                    if (self.gateways.dropcopy.clients.has(username)) {
                        self.gateways.dropcopy.sendMsg(message_template, username, order.data, function(msg) {});
                    }
                });
            }
        }
    }

    var _send_posttrade_message = function(sessions, message_template, data, status, cb) {
        var tradeId = null;
        var i = 0;
        async.whilst(
            function () { return i < sessions.length; },
            function (next) {
                var session = sessions[i];
                var session_instance = session.session;
                var session_account = session.account;

                session_instance.sendMsg(message_template, session_account, data, function(msg) {
                   if (msg != undefined) {
                        tradeId = msg['1003'];
                        var f_trades = self.trades.filter(function(o) { return o.tradeId == tradeId});
                        if (f_trades.length == 0) {
                            var new_trade = {
                                session: session_instance,
                                account: session_account,
                                broker: session_instance.accounts.filter(function(o) { return o.targetID == session_account})[0].brokerid,
                                tradeId: tradeId,
                                data: msg,
                                status: status,
                                messages: []
                            }
                            new_trade.messages.push(msg);
                            self.trades.push(new_trade);
                        } else {
                            var trade = f_trades[0];
                            _updateData(trade, msg);
                            trade.status = status;
                            trade.messages.push(msg);
                        }
                    }
                    i++;
                    next();
                });
            },
            function (err) {
                if (err) console.log(err);
                cb(tradeId);
            }
        );
    }

    var _build_onbook_parties = function(broker) {
        var parties = null;
        var f_parties = self.parties.filter(function(o) { return o.trader == broker});
        if (f_parties.length > 0) {
            var party = f_parties[0];
            parties = {
                "453": [
                    {
                        "448": party.trader,
                        "447": "D",
                        "452": "1"
                    },
                    {
                        "448": party.tradergroup,
                        "447": "D",
                        "452": "53"
                    },
                    {
                        "448": party.firm,
                        "447": "D",
                        "452": "76"
                    }
                ]
            }
        }

        return parties;
    }

    var processNewOrder = function(session, message) {
        var account = message.account;
        var broker = session.accounts.filter(function(o) { return o.username == account})[0].brokerid;
        var status = "CREATE";
        var data = { 'CompID': account, 'ExecutedPrice': '0.000000' }

        var new_order = {
            session: session,
            account: account,
            status: status,
            broker: broker,
            data: data
        };

        _updateData(new_order, message.message);
        self.orders.push(new_order);
    }

    var processAmendOrder = function(session, message) {
        var orderid = message.message['OrderID'];
        var order = _findOrderById(orderid);
        if (order) {
            _updateData(order, message.message);
            order.data.CompID = message.account;
            order.session = session;
            order.account = message.account;
            order.status = "AMEND";
            order.broker = session.accounts.filter(function(o) { return o.username == message.account})[0].brokerid;
        }
    }

    var processCancelOrder = function(session, message) {
        var orderid = message.message['OrderID'];
        var order = _findOrderById(orderid);
        if (order) {
            _updateData(order, message.message);
            order.data.CompID = message.account;
            order.session = session;
            order.account = message.account;
            order.broker = session.accounts.filter(function(o) { return o.username == message.account})[0].brokerid;
            if (order.status == 'CLOSED') {
                order.data.RejectCode = "002000" //  Order not found (too late to cancel or unknown order)
                rejectOrder(order, function() {});
            } else {
                order.status = "CANCEL";
            }

        }
    }

    var processOrderMassCancel = function(session, message) {
        var client = message.account;
        var req_type = parseInt(message.message['MassCancelRequestType']);
        var order_book = parseInt(message.message['OrderBook']);

        if (order_book == 1) { // Regular
            var orders = [];
            var message_template = template.native.ack_order_mass_cancel_report;
            var data = null;
            switch(req_type) {
                case 3: // All Firm orders for instrument
                    var firm = session.accounts.filter(function(o) { return o.username == client })[0].brokerid;
                    var secid = message.message['SecurityID'];
                    orders = self.orders.filter(function(o) { return o.status != "CLOSED" && o.broker == firm && o.data.SecurityID == secid });
                    break;
                case 4: // All Firm orders for Segment
                    var segment = message.message['Segment'];
                    var firm = session.accounts.filter(function(o) { return o.username == client })[0].brokerid;
                    var sec_segment_f = self.instruments.filter(function(o) { return o.sourceexchange == segment });
                    if (sec_segment_f.length > 0) {
                        orders = self.orders.filter(function(o) { return o.status != "CLOSED" && o.broker == firm && sec_segment_f.filter(function(s) { return s.exchangecode == o.data.SecurityID}).length > 0 });
                    }
                    break;
                case 7: // All orders for Client (Interface User ID)
                    orders = self.orders.filter(function(o) { return o.status != "CLOSED" && o.account == client });
                    break;
                case 8: // All orders for Firm
                    var firm = session.accounts.filter(function(o) { return o.username == client })[0].brokerid;
                    orders = self.orders.filter(function(o) { return o.status != "CLOSED" && o.broker == firm });
                    break;
                case 9: // Client (Interface User ID) orders for Instrument
                    var secid = message.message['SecurityID'];
                    orders = self.orders.filter(function(o) { return o.status != "CLOSED" && o.account == client && o.data.SecurityID == secid });
                    break;
                case 14: // Client Interest for Underlying
                    var secid_underlying = message.message['SecurityID'];
                    var sec_underlying_f = self.instruments.filter(function(o) { return o.exchangecode == secid_underlying });
                    if (sec_underlying_f.length > 0) {
                        var sec_underlying = sec_underlying_f[0].symbol;
                        var secs_with_underlying = self.instruments.filter(function(o) { return o.symbol.startsWith(sec_underlying) && o.symbol != secid_underlying });
                        if (secs_with_underlying.length > 0) {
                            var secs_udl = [];
                            secs_with_underlying.forEach(function(sec){
                                secs_udl.push(sec.exchangecode);
                            });
                            orders = self.orders.filter(function(o) { return o.status != "CLOSED" && o.account == client && secs_udl.filter(function(s) { return s == o.data.SecurityID}).length > 0 });
                        }
                    }
                    break;
                case 15: // Client (Interface User ID) orders for Segment
                    var segment = message.message['Segment'];
                    var sec_segment_f = self.instruments.filter(function(o) { return o.sourceexchange == segment });
                    if (sec_segment_f.length > 0) {
                        orders = self.orders.filter(function(o) { return o.status != "CLOSED" && o.account == client && sec_segment_f.filter(function(s) { return s.exchangecode == o.data.SecurityID}).length > 0 });
                    }
                    break;
                case 22: //  Firm Interest for Underlying
                    var firm = session.accounts.filter(function(o) { return o.username == client })[0].brokerid;
                    var secid_underlying = message.message['SecurityID'];
                    var sec_underlying_f = self.instruments.filter(function(o) { return o.exchangecode == secid_underlying });
                    if (sec_underlying_f.length > 0) {
                        var sec_underlying = sec_underlying_f[0].symbol;
                        var secs_with_underlying = self.instruments.filter(function(o) { return o.symbol.startsWith(sec_underlying) && o.symbol != secid_underlying });
                        if (secs_with_underlying.length > 0) {
                            var secs_udl = [];
                            secs_with_underlying.forEach(function(sec){
                                secs_udl.push(sec.exchangecode);
                            });
                            orders = self.orders.filter(function(o) { return o.status != "CLOSED" && o.broker == firm && secs_udl.filter(function(s) { return s == o.data.SecurityID}).length > 0 });
                        }
                    }
                    break;
            }
        } else { // Negotiated Trades
            console.log('Not Implement yet!');
        }
        data = {
            "ClientOrderID": message.message['ClientOrderID'],
            "OrderBook": order_book,
            "Status": 7,
            "RejectCode": ''
        }

        session.sendMsg(message_template, client, data, function(msg) {
            orders.forEach(function(o) {
                o.status = "CANCEL";
            });
        });
    }

    var sendRecoveryMessages = function(session, account, messages, cb) {
        var i = 0;
        cb();

        // async.whilst(
        //     function () { return i < messages.length },
        //     function (next) {
        //         var message = messages[i];
        //         session.sendMsg(message, account, null, function(msg) {
        //             i++;
        //             next();
        //         });
        //     },
        //     function (err) {
        //         if (err) console.log(err);
        //         cb()
        //     }
        // );
    }

    var processMissedMessage = function(session, message) {
        var seqno = message.message['SequenceNumber'];
        var partitionId = message.message['PartitionId'];
        var message_template_ack = template.native.ack_missed_message;
        var message_template_complete = template.native.ack_transmission_complete;
        var account = message.account;
        var data = {};

        if (partitionId == 2) {
            data.Status = 0;
        } else{
            data.Status = 2
        }

        session.sendMsg(message_template_ack, account, data, function(msg) {
            if (data.Status == 0) {
                sendRecoveryMessages(session, account, session.outgoingMessages, function(){
                    session.sendMsg(message_template_complete, account, null, function(msg) {});
                });
            }
        });
    }

    var processJSEMessage = function(msgtype, session, message) {
        switch(msgtype) {
            case "D": // new order
                processNewOrder(session, message);
                break;
            case "G": // amend order
                processAmendOrder(session, message);
                break;
            case "F": // cancel order
                processCancelOrder(session, message);
                break;
            case "S": // Quote
                break;
            case "q": // Order Mass Cancel Request
                processOrderMassCancel(session, message);
                break;
            case "a": // Quote Request
                break;
            case "b": // Quote Request Reject
                break;
            case "d": // RFQ Quote
                break;
            case "f": // Quote Response
                break;
            case "O": // Security Definition Request
                break;
            case "M": // Missed Message Request
                processMissedMessage(session, message);
                break;
        }
    }

    var processOffBookTrade = function(session, account, tcr_message) {
        var tradeType = tcr_message['1123']; // Single Party TCR  or Dual Party TCR
        var tradeReportType = tcr_message['856']; // Submit / Notify / Accept / Cancel / Withdraw / Cancel Withdraw / Decline
        var tradeReportTransType = tcr_message['487']; // New / Cancel / Replace

        // New TCR (Dual / Single)
        if (tradeReportType == 0 && tradeReportTransType == 0) {
            if (tradeType == 1) { // Single party TCR
                newSinglePartyTCR(tcr_message, function() {});
            } else {
                newDualPartyTCR(tcr_message, function() {});
            }
        }

        // Cancel TCR (Dual / Single)
        if (tradeReportType == 6 && tradeReportTransType == 0) {
            var tradeid = tcr_message['1003'];
            var f_trades = self.trades.filter(function(o) { return o.tradeId == tradeid });
            if (f_trades.length > 0) {
                var trade = f_trades[0];
                trade.data['572'] = trade.data['571'];
                _updateData(trade, tcr_message);
                if (tradeType == 1){ // Single party TCR
                    cancelSinglePartyTCR(trade, function() {});
                } else {
                    cancelDualPartyTCR(trade, function() {});
                }
            }
        }

        // Accept TCR  / Accept Cancel TCR (Dual)
        if (tradeReportType == 2 && tradeReportTransType == 2) {
            var tradeId = tcr_message['1003'];
            var f_trades = self.trades.filter(function(o) { return o.tradeId == tradeId });
            if (f_trades.length > 0) {
                var trade = f_trades[0];
                _updateData(trade, tcr_message);
                if (trade.status == "NOTIFIED_CREATE") {
                    acceptDualPartyTCR(trade, function() {});
                } else if (trade.status == "NOTIFIED_CANCEL") {
                    acceptCancelDualPartyTCR(trade, function() {});
                } else if (trade.status == "NOTIFIED_CANCEL_WITHDRAW") {
                    rejectTCR(trade, '7050', 'Cancellation process terminated');
                } else if (trade.status == "NOTIFIED_WITHDRAW") {
                    rejectTCR(trade, '7060', 'Request already accepted/declined');
                }
            }
        }

        // Decline TCR / Reject Cancel TCR(Dual)
        if (tradeReportType == 3 && tradeReportTransType == 2) {
            var tradeId = tcr_message['1003'];
            var f_trades = self.trades.filter(function(o) { return o.tradeId == tradeId });
            if (f_trades.length > 0) {
                var trade = f_trades[0];
                _updateData(trade, tcr_message);
                if (trade.status == "NOTIFIED_CANCEL") {
                    rejectCancelDualPartyTCR(trade, function() {});
                } else {
                    declineDualPartyTCR(trade, function() {});
                }
            }
        }

        // WithDraw TCR (Dual)
        if (tradeReportType == 0 && tradeReportTransType == 1) {
            var tradeId = tcr_message['1003'];
            var f_trades = self.trades.filter(function(o) { return o.tradeId == tradeId });
            if (f_trades.length > 0) {
                var trade = f_trades[0];
                _updateData(trade, tcr_message);
                withdrawDualPartyTCR(trade, function() {});
            }
        }

        // WithDraw TCR cancellation (Dual)
        if (tradeReportType == 6 && tradeReportTransType == 1) {
            var tradeId = tcr_message['1003'];
            var f_trades = self.trades.filter(function(o) { return o.tradeId == tradeId });
            if (f_trades.length > 0) {
                var trade = f_trades[0];
                _updateData(trade, tcr_message);
                withdrawCancelDualPartyTCR(trade, function() {});
            }
        }
    }

    var processOnBookTrade = function(session, account, tcr_message) {
        var tradeReportType = tcr_message['856']; // Submit / Notify / Accept / Cancel / Withdraw / Cancel Withdraw / Decline
        var tradeReportTransType = tcr_message['487']; // New / Cancel / Replace

        // Cancel Trade
        if (tradeReportType == 6 && tradeReportTransType == 0) {
            var tradeid = tcr_message['1003'];
            var side = tcr_message['552'][0]['54'];
            var f_orders = self.orders.filter(function(o) { return o.trade != undefined && o.trade.tradeId == tradeid && o.data.Side == side });
            if (f_orders.length > 0) {
                var order = f_orders[0];
                _updateData(order.trade, tcr_message);
                cancelOnBookTrade(order, function() {});
            }
        }
    }

    var processTradeCaptureReprort = function(session, message) {
        var tcr_message = message.message;
        var account = message.account;
        var tradeType = tcr_message['1123'];

        if (tradeType == undefined) { // Onbook Trade
            processOnBookTrade(session, account, tcr_message);
        } else {
            processOffBookTrade(session, account, tcr_message);
        }
    }

    var processOrderMassStatusRequest = function(session, message) {
        var msg = message.message;
        var broker = msg['453'][0]['448'];
        var massStatusReqID = msg['584'];
        var f_orders = self.orders.filter(function(o) { return o.status != 'CLOSED' && o.broker == broker });

        var parties = _build_onbook_parties(broker);
        var count = f_orders.length;
        var index = 1;
        f_orders.forEach(function(order) {
            var message_template = JSON.parse(JSON.stringify(template.fix.dc_order_status));
            message_template['584'] = massStatusReqID;
            if (index == count){
                message_template['912'] = 'Y';
            }
            message_template = _.extend({}, message_template, parties);
            session.sendMsg(message_template, message.account, order.data, function(data){});
            index = index + 1;
        })
    }

    var processTradeCaptureReportRequest = function(session, message) {
        var msg = message.message;
        var account = message.account;
        var tradeRequestID = msg['568'];
        var username = msg['49'];

        var trade_messages = [];

        self.trades.forEach(function(trade) {
            var valid_messages = trade.messages.filter(function(o) { return o['35'] == 'AE' && o['56'] == username });
            trade_messages = trade_messages.concat(valid_messages);
        });
        var totNumTradeReports = trade_messages.length;

        var ack_data = {
            '568': tradeRequestID,
            '748': totNumTradeReports
        }

        var index = 1;
        session.sendMsg(template.fix.pt_ack_tcr_request, account, ack_data, function(data){
            if (totNumTradeReports > 0) {
                trade_messages.forEach(function(trade){
                    var message_template = trade;
                    message_template['568'] = tradeRequestID;
                    if (index == totNumTradeReports){
                        message_template['912'] = 'Y';
                    }
                    session.sendMsg(message_template, account, null, function(data){});
                    index = index + 1;
                });
            }
        });
    }

    var processFIXMessage = function(msgtype, session, message) {
        switch(msgtype) {
            case "AE": // PostTrade - Trade Capture Report
                processTradeCaptureReprort(session, message);
                break;
            case "AF": // DropCopy - Order Mass Status Request
                processOrderMassStatusRequest(session, message);
                break;
            case "AD": // PostTrade - Trade Capture Report Request
                processTradeCaptureReportRequest(session, message);
                break;
        }
    }


    var publishNews = function(session, account, news) {
        var news = {
            "OrigTime": moment.utc().format("hh:mm:ss"),
            "Urgency": news.urgency,
            "Headline": news.headline,
            "Text": news.text,
            "Instruments": news.instruments,
            "UnderlyingInstruments": news.underlyings,
            "FirmList": news.firmlist,
            "UserList": news.userlist,
        }

        session.sendMsg(template.native.news, account, news, function(data) {});
    }

    // ***********************************************************************
    self.process = function(protocol, msgtype, session, message) {
        switch(protocol) {
            case "JSE":
                processJSEMessage(msgtype, session, message);
                break;
            case "FIX":
                processFIXMessage(msgtype, session, message);
                break;
        }
    }

    self.publish_news = function(session, news, cb) {
        var firms = [];
        var clients = [];
        if (news.firmlist != undefined) {
            firms = news.firmlist.split('|').filter(function(el) {return el.length != 0});
        }
        if (news.userlist != undefined) {
            clients = news.userlist.split('|').filter(function(el) {return el.length != 0});
        }
        session.accounts.forEach(function(account) {
            var firm = account.brokerid;
            var client = account.username;

            if (firms.length > 0) {
                if (firms.indexOf(firm) >= 0) {
                    if (clients.length > 0) {
                        if (clients.indexOf(client) >= 0) {
                            publishNews(session, client, news);
                        }
                    } else {
                        publishNews(session, client, news);
                    }
                }
            } else {
                publishNews(session, client, news);
            }
        });
        cb();
    }

    self.clearIntervals = function(cb) {
        clearInterval(timer_create_orders);
        clearInterval(timer_amend_orders);
        clearInterval(timer_cancel_orders);
        clearInterval(timer_timing_orders);
        clearInterval(timer_stop_orders);
        clearInterval(timer_trailing_orders);
        cb();
    }
}
