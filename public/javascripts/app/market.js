app.controller('ctrlMarket', ['$scope','$routeParams', '$http', 'moment','Notification', 'fileReader', 'FileSaver', 'Blob',
    function($scope, $routeParams, $http, moment, Notification, fileReader, FileSaver, Blob) {
    $scope.market = null;

    $scope.markettypes = [
        {value: 1, text: 'MIT'},
        {value: 2, text: 'JSEDERIV'}
    ];

    $scope.new_mit_market = {
        id: null,
        createdtime : null,
        name: "Untitled Market",
        description: null,
        type: 'MIT',
        isrunning: false,
        parties: JSON.stringify([
            {
                "trader":"PEREZA1XXX",
                "tradergroup":"80016",
                "firm":"PERRSTHER01"
            },
            {
                "trader":"PEREZA2CDS",
                "tradergroup":"80033",
                "firm":"PYSRSTHER01"
            }
        ]),
        gateways : {
            orderentry: {
                spec: 'mit.3.0.1',
                port: 21501,
                recoveryport: 21502,
                accounts: JSON.stringify([
                    {
                        username: "PERN01",
                        password: "iress123",
                        brokerid: "PEREZA1XXX"
                    },
                    {
                        username: "PERN02",
                        password: "iress123",
                        brokerid: "PEREZA1XXX"
                    },
                    {
                        username: "PYSN01",
                        password: "iress123",
                        brokerid: "PEREZA2CDS"
                    },
                    {
                        username: "PYSN02",
                        password: "iress123",
                        brokerid: "PEREZA2CDS"
                    }
                ])
            },
            dropcopy: {
                fixversion: 'FIXT.1.1',
                spec: 'fix.5.0.2.mit',
                port: '21581',
                options: JSON.stringify({
                    responseLogonExtensionTags: {"1137":"9","1409":"0"},
                    responseLogoutExtensionTags:{"1409":"4"}
                }),
                accounts: JSON.stringify([
                    {
                        senderID: "JSEDCPGW",
                        targetID: "PERD01",
                        password: "iress123",
                        brokerid: "PEREZA1XXX"
                    },
                    {
                        senderID: "JSEDCPGW",
                        targetID: "PERD02",
                        password: "iress123",
                        brokerid: "PEREZA1XXX"
                    },
                    {
                        senderID: "JSEDCPGW",
                        targetID: "PYSD01",
                        password: "iress123",
                        brokerid: "PEREZA2CDS"
                    },
                    {
                        senderID: "JSEDCPGW",
                        targetID: "PYSD02",
                        password: "iress123",
                        brokerid: "PEREZA2CDS"
                    }
                ])
            },
            posttrade:{
                fixversion: 'FIXT.1.1',
                spec: 'fix.5.0.2.mit',
                port: '21561',
                options: JSON.stringify({
                    responseLogonExtensionTags: {"1137":"9","1409":"0"},
                    responseLogoutExtensionTags:{"1409":"4"}
                }),
                accounts: JSON.stringify([
                    {
                        senderID: "JSEPTPGW",
                        targetID: "PERP01",
                        password: "iress123",
                        brokerid: "PEREZA1XXX",
                        trader: "80016",
                        tradergroup: "PERRSTHER01"
                    },
                    {
                        senderID: "JSEPTPGW",
                        targetID: "PERP02",
                        password: "iress123",
                        brokerid: "PEREZA1XXX",
                        trader: "80016",
                        tradergroup: "PERRSTHER01"
                    },
                    {
                        senderID: "JSEPTPGW",
                        targetID: "PYSP01",
                        password: "iress123",
                        brokerid: "PEREZA2CDS",
                        trader: "80033",
                        tradergroup: "PYSRSTHER01"
                    },
                    {
                        senderID: "JSEPTPGW",
                        targetID: "PYSP02",
                        password: "iress123",
                        brokerid: "PEREZA2CDS",
                        trader: "80033",
                        tradergroup: "PYSRSTHER01"
                    }
                ])
            }
        }
    }

    var isJSON = function(str) {
        try {
            JSON.parse(str);
        } catch (e) {
            return false;
        }
        return true;
    }

    var getMarket = function(){
        $http.post('/market/', {
        }).then(function(resp) {
            if (resp.data) {
                $scope.market = resp.data;
            }
        }, function(err) {
            Notification({message: 'Error:'+ err.data.error, delay: 2000});
        });
    }

    var loadInstruments = function(file) {
        $http.post('/market/instruments', {
            "file": file
        }).then(function(resp) {
            if (resp.data) {
                $scope.market.instruments = resp.data;
            }
        }, function(err) {
            Notification({message: 'Error:'+ err.data.error, delay: 2000});
        });
    }

    getMarket();

    $scope.getFile = function () {
        fileReader.readAsDataUrl($scope.file, $scope)
                  .then(function(result) {
                        if (isJSON(result))
                        {
                            $scope.market = JSON.parse(result);
                            if ($scope.market.instrument_file != undefined)
                                loadInstruments($scope.market.instrument_file);
                        } else {
                            Notification({message: 'Market setting file is invalid JSON', delay: 2000});
                        }
                  });
    };

    $scope.newMarket = function(type) {
        switch(type) {
            case 'MIT':
                $scope.market = angular.copy($scope.new_mit_market);
                break;
            case 'JSEDERIV':
                $scope.market = angular.copy($scope.new_edm_market);
                break;
        }
    }

    $scope.saveMarket = function() {
        $scope.market.createdtime = moment();
        var market_data = JSON.stringify($scope.market);
        var data = new Blob([market_data], { type: 'application/json;charset=utf-8' });
        FileSaver.saveAs(data, "mysetting.json");
    }

    $scope.startMarket = function(market) {
        $http.post('/market/start', {
            "market": market
        }).then(function(resp) {
            if (resp.data) {
                $scope.market.isrunning = true;
            }
        }, function(err) {
            Notification({message: 'Error:'+ err.data.error, delay: 2000});
        });
    }

    $scope.stopMarket = function(market) {
        $http.post('/market/stop', {
        }).then(function(resp) {
            if (resp.data) {
                $scope.market.isrunning = false;
            }
        }, function(err) {
            Notification({message: 'Error:'+ err.data.error, delay: 2000});
        });
    }
}]);




