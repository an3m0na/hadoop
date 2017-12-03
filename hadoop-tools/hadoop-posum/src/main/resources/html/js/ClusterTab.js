function ClusterTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;

  self.refresh = function () {
    self.loading = true;
    var path = env.isTest ? "mocks/dmmetrics_all-cluster.json" : self.comm.paths.DM + "/all-cluster?since=" + self.lastRefreshed;
    self.comm.requestData(path, function (data) {
      self.lastRefreshed = data.time;
      updateTimeSeriesPlot(self, "plot_apps", data, {
        entryValueExtractor: function (entry) {
          var ret = {};
          $.each(entry.queues, function (name, queue) {
            ret["Running in " + name] = queue.applications.running;
            ret["Total in " + name] = queue.applications.running + queue.applications.pending;
          });
          return ret;
        },
        plotTitle: "Applications",
        yaxis: {title: "Number"},
        baseTime: env.isTest ? env.testTime : 0
      });
      updateTimeSeriesPlot(self, "plot_containers", data, {
        entryValueExtractor: function (entry) {
          var ret = {};
          $.each(entry.queues, function (name, queue) {
            ret[name] = queue.containers;
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
          $.each(entry.resources, function (hostname, resources) {
            ret[hostname] = resources.avail.memory;
          });
          return ret;
        },
        plotTitle: "Free Resources",
        yaxis: {title: "Memory (GB)"},
        baseTime: env.isTest ? env.testTime : 0
      });

      self.loading = false;
    }, function () {
      self.loading = false;
    });
  };
}

ClusterTab.prototype = Object.create(Tab.prototype);