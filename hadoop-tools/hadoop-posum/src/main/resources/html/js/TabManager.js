function TabManager(env) {
  var self = this;
  var tabContainers = $(".an3-tab");
  var customTabs = {
    cluster: "ClusterTab",
    system: "SystemTab",
    scheduler: "SchedulerTab",
    performance: "PerformanceTab",
    logs: "LogsTab",
    controls: "ControlsTab",
    analysis: "AnalysisTab"
  };
  var tabs = {};
  var navBar = $("#navbar");
  var navItems = navBar.find("li");

  self.switchToTab = function (name) {
    tabContainers.hide();
    var tab = tabs[name];
    if (!tab)
      tabs.none.container.show();
    else {
      tab.container.show();
      tab.refresh();
    }
  };

  self.initialize = function () {
    $(".an3-nav-link").on("click", function () {
        var link = $(this);
        var div = link.attr("href");
        var newState = div.substr(1);
        self.changeState(newState, link.attr("id") === "btn_start" ? null : link);
      }
    );

    tabContainers.each(function () {
      var container = $(this);
      var id = container.attr("id");
      var customTab = customTabs[id] || "Tab";
      tabs[id] = new window[customTab](id, container, env);
    });

    setInterval(function () {
      env.testTime += env.refreshInterval;
      var tab = tabs[env.state];
      if (tab.loading)
        return;
      tab.refresh();
    }, env.refreshInterval);

    return self;
  };

  self.changeState = function (newState, link) {
    navItems.removeClass("active");
    if (!link)
      link = navItems.find(".an3-nav-link[href=\\#" + newState + "]");
    link.parent().addClass("active");

    env.state = newState;
    self.switchToTab(newState);
  };

  self.time = 0;
}