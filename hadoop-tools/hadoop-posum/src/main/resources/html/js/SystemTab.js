function SystemTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.updateSystemCharts = function (componentAcronym, componentName, data) {
    var plotPrefix = "plot_" + componentAcronym.toLowerCase();
    var commonConfig = {
      listExtractor: function (data) {
        return data[componentAcronym];
      },
      baseTime: env.isTest ? env.testTime : 0
    };
    updateTimeSeriesPlot(self, plotPrefix + "_jvm", data, $.extend({}, commonConfig, {
      entryValueExtractor: function (entry) {
        return {Total: entry.jvm.total, Max: entry.jvm.max, Used: entry.jvm.used};
      },
      plotTitle: "JVM Memory for " + componentName,
      yaxis: {title: "Memory (GB)"}
    }));
    updateTimeSeriesPlot(self, plotPrefix + "_cpu", data, $.extend({}, commonConfig, {
      entryValueExtractor: function (entry) {
        return {Total: entry.cpu.total, Process: entry.cpu.process};
      },
      plotTitle: "CPU Load for " + componentName,
      yaxis: {title: "Fraction (%)"}
    }));
    updateTimeSeriesPlot(self, plotPrefix + "_threads", data, $.extend({}, commonConfig, {
      entryValueExtractor: function (entry) {
        return {Threads: entry.threadCount};
      },
      plotTitle: "Active Threads for " + componentName,
      yaxis: {title: "Total Number"}
    }));
  };
  self.activate = function () {
    var path = env.isTest ? "js/metrics_system.json" : self.comm.dmPath + "/all-system";
    self.comm.requestData(path, function (data) {
      self.updateSystemCharts("PS", "Portfolio Scheduler", data);
      self.updateSystemCharts("OM", "Orchestration Master", data);
      self.updateSystemCharts("DM", "Data Master", data);
      self.updateSystemCharts("SM", "Simulation Master", data);
    });
  };
}

SystemTab.prototype = Object.create(Tab.prototype);