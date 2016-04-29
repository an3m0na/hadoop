
$(document).ready(function () {
    var env = {state: "home"};
    var comm = new Communicator(env).initialize();
    var tabManager = new TabManager(env).initialize();
    env.comm = comm;
    env.tabManager = tabManager;

    if (location.hash && location.hash.length > 1)
        env.state = location.hash.substr(1);

    $("#div_title").on("click", function () {
        window.location = "";
    });

    tabManager.changeState(env.state);
});
