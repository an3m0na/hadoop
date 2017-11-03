function PerformanceTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.activate = function () {
    var path = env.isTest ? "mocks/dmmetrics_performance.json" : self.comm.dmPath + "/performance";
    self.comm.requestData(path, function (data) {
      updateTimeSeriesPlot(self, "plot_performance_slowdown", data, {
        entryValueExtractor: function (entry) {
          return {Slowdown: entry.score.slowdown};
        },
        plotTitle: "Slowdown",
        yaxis: {title: "Ratio"},
        baseTime: env.isTest ? env.testTime : 0
      });
      updateTimeSeriesPlot(self, "plot_performance_penalty", data, {
        entryValueExtractor: function (entry) {
          return {Penalty: entry.score.penalty};
        },
        plotTitle: "Deadline Penalty",
        yaxis: {title: "Value"},
        baseTime: env.isTest ? env.testTime : 0
      });
      updateTimeSeriesPlot(self, "plot_performance_cost", data, {
        entryValueExtractor: function (entry) {
          return {Cost: entry.score.cost};
        },
        plotTitle: "Cost",
        yaxis: {title: "Cost (Euro)"},
        baseTime: env.isTest ? env.testTime : 0
      });
    });
  };
}

PerformanceTab.prototype = Object.create(Tab.prototype);