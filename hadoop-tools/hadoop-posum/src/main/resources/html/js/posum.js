$(document).ready(function () {
  var env = {state: "home", isTest: false, refreshInterval: 5000, testTime: 0};

  if (location.hash && location.hash.length > 1)
    env.state = location.hash.substr(1);

  $("#div_title").on("click", function () {
    window.location = "";
  });

  env.comm = new Communicator(env).initialize();
  var tabManager = new TabManager(env).initialize();
  env.tabManager = tabManager;

  var path = env.isTest ? "mocks/conf.json" : env.comm.paths.OM + "/conf";
  var confComplete = true;
  var periodicUpdater;

  var updateConf = function (data) {
    $.each(data.addresses, function (component, address) {
      if (!address) {
        confComplete = false;
        return;
      }
      env.comm.paths[component] = "http://" + address + "/ajax";
    });
    if (confComplete)
      clearInterval(periodicUpdater);
  };
  env.comm.requestData(path, function (data) {
    updateConf(data);
    if (!confComplete) {
      env.comm.showDialog("Warning", "Not all POSUM processes are online yet.\n\nData may not be complete.");
      periodicUpdater = setInterval(function () {
        env.comm.requestData(path, updateConf);
      }, env.refreshInterval);
    }
  });

  tabManager.changeState(env.state);
});
