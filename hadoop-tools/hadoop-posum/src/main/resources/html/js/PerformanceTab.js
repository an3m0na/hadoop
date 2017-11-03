function PerformanceTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.activate = function () {
    var path = env.isTest ? "js/dmmetrics_performance.json" : self.comm.dmPath + "/performance";
    self.comm.requestData(path, function (data) {
      updateTimeSeriesPlot(self, "plot_performance_slowdown", data, {
        listExtractor: function (data) {
          return data.scores
        },
        entryValueExtractor: function (entry) {
          return {Slowdown: entry.score.slowdown};
        },
        plotTitle: "Slowdown",
        yaxis: {title: "Ratio", tickmode: "auto", nticks: 10},
        baseTime: env.isTest ? env.testTime : 0
      });
      updateTimeSeriesPlot(self, "plot_performance_penalty", data, {
        listExtractor: function (data) {
          return data.scores
        },
        entryValueExtractor: function (entry) {
          return {Penalty: entry.score.penalty};
        },
        plotTitle: "Deadline Penalty",
        yaxis: {title: "Value", tickmode: "auto", nticks: 10},
        baseTime: env.isTest ? env.testTime : 0
      });
      updateTimeSeriesPlot(self, "plot_performance_cost", data, {
        listExtractor: function (data) {
          return data.scores
        },
        entryValueExtractor: function (entry) {
          return {Cost: entry.score.cost};
        },
        plotTitle: "Cost",
        yaxis: {title: "Cost (Euro)", tickmode: "auto", nticks: 10},
        baseTime: env.isTest ? env.testTime : 0
      });
    });
  };
}

PerformanceTab.prototype = Object.create(Tab.prototype);