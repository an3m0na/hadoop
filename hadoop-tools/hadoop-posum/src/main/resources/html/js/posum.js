
$(document).ready(function () {
    var env = {state: "home", isTest:false, refreshInterval:2000};

    if (location.hash && location.hash.length > 1)
        env.state = location.hash.substr(1);

    $("#div_title").on("click", function () {
        window.location = "";
    });

    var comm = new Communicator(env).initialize();
    var tabManager = new TabManager(env).initialize();
    env.comm = comm;
    env.tabManager = tabManager;

    tabManager.changeState(env.state);
});
