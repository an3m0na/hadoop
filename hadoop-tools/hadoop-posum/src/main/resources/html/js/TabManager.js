function TabManager(env) {
  var self = this;
  var tabContainers = $(".an3-tab");
  var tabHandlers = {
    none: function (id, container, env) {
      return new Tab(id, container, env);
    },
    home: function (id, container, env) {
      return new Tab(id, container, env);
    },
    cluster: function (id, container, env) {
      return new ClusterTab(id, container, env);
    },
    system: function (id, container, env) {
      return new SystemTab(id, container, env);
    },
    scheduler: function (id, container, env) {
      return new SchedulerTab(id, container, env);
    },
    performance: function (id, container, env) {
      return new PerformanceTab(id, container, env);
    },
    logs: function (id, container, env) {
      return new LogsTab(id, container, env)
    }
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

    tabContainers.each(function (i, e) {
      var container = $(e);
      var id = container.attr("id");
      tabs[id] = tabHandlers[id](id, container, env);
    });

    setInterval(function () {
      env.testTime += env.refreshInterval;
      tabs[env.state].activate();
    }, env.refreshInterval);

    return self;
  };

  self.changeState = function (newState, link) {
    navItems.removeClass("active");
    if (!link)
      link = navItems.find(".an3-nav-link[href=#" + newState + "]");
    link.parent().addClass("active");

    env.state = newState;
    self.switchToTab(newState);
  };

  self.time = 0;
}