app.controller('ctrlMarket', ['$scope','$routeParams', '$http', 'moment','Notification', 'fileReader', 'FileSaver', 'Blob', '$uibModal',
    function($scope, $routeParams, $http, moment, Notification, fileReader, FileSaver, Blob, $uibModal) {
    $scope.market = null;

    var showForm = function (settings, callback) {
        var modalInstance = $uibModal.open({
            animation: true,
            templateUrl: 'views/modal-settings.html',
            controller: 'SettingsModalCtrl',
            size: 'lg',
            scope: $scope,
            resolve: {
                settingsForm: function () {
                    return $scope.settingsForm;
                },
                settings: function() {
                    if (settings != undefined)
                        return settings;
                    else
                        return {};
                }
            }
        });

        modalInstance.result.then(function (result) {
            callback(result);
        }, null);
    };

    $scope.new_mit_market = {
        isrunning: false,
        createdtime : null,
        type: "MIT",
        name: "Untitled MIT Market",
        description: null,
        instrument_file: "mit.csv",
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
                        username: "PERN05",
                        password: "iress123",
                        brokerid: "PEREZA1XXX"
                    },
                    {
                        username: "PYSN06",
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
                        targetID: "PERD05",
                        password: "iress123",
                        brokerid: "PEREZA1XXX"
                    },
                    {
                        senderID: "JSEDCPGW",
                        targetID: "PYSD06",
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
                        targetID: "PERP05",
                        password: "iress123",
                        brokerid: "PEREZA1XXX",
                        trader: "80016",
                        tradergroup: "PERRSTHER01"
                    },
                    {
                        senderID: "JSEPTPGW",
                        targetID: "PYSP06",
                        password: "iress123",
                        brokerid: "PEREZA2CDS",
                        trader: "80033",
                        tradergroup: "PYSRSTHER01"
                    }
                ])
            }
        }
    }

    $scope.new_edm_market = {
        createdtime : null,
        isrunning: false,
        type: "JSEDERIV",
        name: "Untitled JSEDERIV Market",
        description: null,
        instrument_file: "mit.csv",
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
                spec: 'edm.3.0.4',
                port: 41509,
                recoveryport: 41510,
                accounts: JSON.stringify([
                    {
                        username: "PERN01",
                        password: "iress123",
                        brokerid: "PEREZA1XXX"
                    },
                    {
                        username: "PERN05",
                        password: "iress123",
                        brokerid: "PEREZA1XXX"
                    },
                    {
                        username: "PYSN06",
                        password: "iress123",
                        brokerid: "PEREZA2CDS"
                    }
                ])
            },
            dropcopy: {
                fixversion: 'FIXT.1.1',
                spec: 'fix.5.0.2.edm',
                port: 41561,
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
                        targetID: "PERD05",
                        password: "iress123",
                        brokerid: "PEREZA1XXX"
                    },
                    {
                        senderID: "JSEDCPGW",
                        targetID: "PYSD06",
                        password: "iress123",
                        brokerid: "PEREZA2CDS"
                    }
                ])
            },
            posttrade:{
                fixversion: 'FIXT.1.1',
                spec: 'fix.5.0.2.edm',
                port: 41581,
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
                        targetID: "PERP05",
                        password: "iress123",
                        brokerid: "PEREZA1XXX",
                        trader: "80016",
                        tradergroup: "PERRSTHER01"
                    },
                    {
                        senderID: "JSEPTPGW",
                        targetID: "PYSP06",
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

    var saveMarket = function() {
        var data = new Blob([JSON.stringify($scope.market)], { type: 'application/json;charset=utf-8' });
        FileSaver.saveAs(data, "mysetting.json");
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
        var newmarket = null;
        switch(type) {
            case 'MIT':
                newmarket = angular.copy($scope.new_mit_market);
                break;
            case 'JSEDERIV':
                newmarket = angular.copy($scope.new_edm_market);
                break;
        }

        showForm(newmarket, function(result){
            if (result != undefined)
            {
                $scope.market = result;
                saveMarket();
            }
        });
    }

    $scope.editMarket = function(market) {
        showForm(market, function(result){
            if (result != undefined)
            {
                $scope.market = result;
                saveMarket();
            }
        });
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




