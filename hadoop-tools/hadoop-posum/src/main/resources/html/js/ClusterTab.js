function ClusterTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.activate = function () {
    var path = env.isTest ? "mocks/dmmetrics_all-cluster.json" : self.comm.dmPath + "/all-cluster";
    self.comm.requestData(path, function (data) {
      updateTimeSeriesPlot(self, "plot_apps", data, {
        entryValueExtractor: function (entry) {
          var ret = {};
          $.each(entry.running, function(queue, running){
            ret[queue] = running.applications;
          });
          return ret;
        },
        plotTitle: "Running Applications",
        yaxis: {title: "Number"},
        baseTime: env.isTest ? env.testTime : 0
      });
      updateTimeSeriesPlot(self, "plot_containers", data, {
        entryValueExtractor: function (entry) {
          var ret = {};
          $.each(entry.running, function(queue, running){
            ret[queue] = running.containers;
          });
          return ret;
        },
        plotTitle: "Running Containers",
        yaxis: {title: "Number"},
        baseTime: env.isTest ? env.testTime : 0
      });
      updateTimeSeriesPlot(self, "plot_resources", data, {
        entryValueExtractor: function (entry) {
          var ret = {};
          $.each(entry.resources, function(hostname, resources){
            ret[hostname] = resources.avail.memory;
          });
          return ret;
        },
        plotTitle: "Free Resources",
        yaxis: {title: "Memory (GB)"},
        baseTime: env.isTest ? env.testTime : 0
      });
    });
  };
}

ClusterTab.prototype = Object.create(Tab.prototype);