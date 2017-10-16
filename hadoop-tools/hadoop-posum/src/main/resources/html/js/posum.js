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

  var path = env.isTest ? "js/conf.json" : env.comm.masterPath + "/conf";
  env.comm.requestData(path, function (data) {
    if (window.location.hostname !== "localhost") {
      var address = data.addresses.PS;
      if (address) {
        env.comm.psPath = "http://" + address + "/ajax";
      } else {
        env.comm.showDialog("Error", "Error occurred:\n" +
          "POSUM not yet ready. Please refresh.");
        return;
      }
      address = data.addresses.DM;
      if (address) {
        env.comm.dmPath = "http://" + address + "/ajax";
      } else {
        env.comm.showDialog("Error", "Error occurred:\n" +
          "POSUM not yet ready. Please refresh.");
        return;
      }
      address = data.addresses.SM;
      if (address) {
        env.comm.smPath = "http://" + address + "/ajax";
      } else {
        env.comm.showDialog("Error", "Error occurred:\n" +
          "POSUM not yet ready. Please refresh.");
      }
    }
  });

  tabManager.changeState(env.state);
});
