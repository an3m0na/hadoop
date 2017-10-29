function ClusterTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.activate = function () {
    var path = env.isTest ? "js/psmetrics_cluster.json" : self.comm.psPath + "/cluster";
    self.comm.requestData(path, function (data) {
      updateTimeSeries(self,
        "plot_apps",
        data,
        function (data) {
          return data.running
        },
        function (traceObject) {
          return traceObject.applications
        },
        "Running Applications",
        {title: "Number", tickmode: "linear"},
        env.isTest ? env.testTime : 0
      );

      updateTimeSeries(self,
        "plot_containers",
        data,
        function (data) {
          return data.running
        },
        function (traceObject) {
          return traceObject.containers
        },
        "Running Containers",
        {title: "Number", tickmode: "linear"},
        env.isTest ? env.testTime : 0
      );
    });
  };
}

ClusterTab.prototype = Object.create(Tab.prototype);