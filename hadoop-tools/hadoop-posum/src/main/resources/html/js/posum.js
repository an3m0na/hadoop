$(document).ready(function () {
    var env = {state: "home", isTest: false, refreshInterval: 2000, testTime: 0};

    if (location.hash && location.hash.length > 1)
        env.state = location.hash.substr(1);

    $("#div_title").on("click", function () {
        window.location = "";
    });

    var comm = new Communicator(env).initialize();
    var tabManager = new TabManager(env).initialize();
    env.comm = comm;
    env.tabManager = tabManager;

    var path = env.isTest ? "/html/js/conf.json" : env.comm.masterPath + "/conf";
    env.comm.requestData(path, function (data) {
        if (window.location.hostname != "localhost") {
            var address = data.psAddress;
            if (address) {
                env.comm.psPath = "http://" + address + "/ajax";
            } else {
                env.comm.showDialog("Error", "Error occurred:\n" +
                    "POSUM not yet ready. Please refresh.");
                return;
            }
            address = data.dmAddress;
            if (address) {
                env.comm.dmPath = "http://" + address + "/ajax";
            } else {
                env.comm.showDialog("Error", "Error occurred:\n" +
                    "POSUM not yet ready. Please refresh.");
                return;
            }
            address = data.smAddress;
            if (address) {
                env.comm.smPath = "http://" + address + "/ajax";
            } else {
                env.comm.showDialog("Error", "Error occurred:\n" +
                    "POSUM not yet ready. Please refresh.");
                return;
            }
        }
    });

    tabManager.changeState(env.state);
});
