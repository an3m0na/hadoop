function SchedulerTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;

  self.refresh = function () {
    self.loading = true;
    var path = env.isTest ? "mocks/dmmetrics_policies.json" : self.comm.paths.DM + "/policies?since=" + self.lastRefreshed;
    self.comm.requestData(path, function (data) {
      self.lastRefreshed = data.time;
      var totalTime = 0;
      var chartData = [];
      var crtColor = 0;
      $.each(data.distribution, function (k, v) {
        totalTime += v.time
      });
      $.each(data.distribution, function (k, v) {
        var trace = {
          x: [Math.round(v.time / totalTime * 100)],
          y: ["Policy"],
          name: k,
          orientation: "h",
          marker: {
            color: chartColors[crtColor++],
            width: 1
          },
          type: "bar"
        };
        chartData.push(trace);
      });
      var layout = {
        title: "Policy Usage",
        barmode: "stack",
        xaxis: {
          tickmode: "linear",
          tick0: 0,
          dtick: 10,
          title: "Percentage of time spent running policy"
        }
      };
      Plotly.newPlot('plot_policy_distribution', chartData, layout);

      updateTimeSeriesPlot(self, "plot_policy_choices", data, {
        entryValueExtractor: function (entry) {
          return {"Choice": entry.policy};
        },
        traceFactory: function () {
          return {mode: "lines+markers", line: {shape: "hv"}}
        },
        plotTitle: "Policy Choices",
        yaxis: {title: "Policy"},
        baseTime: env.isTest ? env.testTime : 0
      });

      self.loading = false
    }, function(){
      self.loading = false;
    });

    path = env.isTest ? "mocks/psmetrics_scheduler.json" : self.comm.paths.PS + "/scheduler";
    updateTimeSeriesPlot(self, "plot_timecost", path, {
      listExtractor: function (data) {
        return [data];
      },
      entryValueExtractor: function (entry) {
        return entry.timecost;
      },
      plotTitle: "Operation Timecost",
      yaxis: {title: "Cost (MS)"},
      baseTime: env.isTest ? env.testTime : 0
    });
  };
}

SchedulerTab.prototype = Object.create(Tab.prototype);