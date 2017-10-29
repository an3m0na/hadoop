function Tab(id, container, env) {
  var self = this;
  self.id = id;
  self.container = container;
  self.plots = {};
  self.lastRefreshed = 0;
  self.comm = env.comm;

  self.activate = function () {
    console.log("Tab ", self.id, " is active");
  }
}