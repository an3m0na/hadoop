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
  env.comm.requestData(path, function (data) {
    if (window.location.hostname !== "localhost") {
      $.each(data.addresses, function (component, address) {
        if (!address) {
          env.comm.showDialog("Error", "Error occurred:\n" +
            "POSUM not yet ready. Please refresh.");
          return;
        }
        env.comm.paths[component] = "http://" + address + "/ajax";
      });
    }
  });

  tabManager.changeState(env.state);
});
