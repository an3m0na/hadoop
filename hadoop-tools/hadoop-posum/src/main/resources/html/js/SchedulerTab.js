function SchedulerTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.activate = function () {
    var path = env.isTest ? "js/dmmetrics_policies.json" : self.comm.dmPath + "/policies";
    self.comm.requestData(path, function (data) {
      //plot_policies_map
      var totalTime = 0;
      var chartData = [];
      var crtColor = 0;
      $.each(data.policies.map, function (k, v) {
        totalTime += v.time
      });
      $.each(data.policies.map, function (k, v) {
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
      Plotly.newPlot('plot_policies_map', chartData, layout);

      //plot_policies_list
      var choiceList = data.policies.list;
      traces = [{
        x: choiceList.times,
        y: choiceList.policies,
        mode: "lines+markers",
        line: {shape: "hv"},
        type: "scatter"
      }];
      layout = {
        title: "Policy Choices",
        xaxis: {
          title: "Time",
          type: "date"
        },
        yaxis: {
          title: "Policy"
        }
      };
      Plotly.newPlot("plot_policies_list", traces, layout);
    });

    path = env.isTest ? "js/psmetrics_scheduler.json" : self.comm.psPath + "/scheduler";
    env.comm.requestData(path, function (data) {
      updateTimeSeries(self,
        "plot_timecost",
        data,
        function (data) {
          return data.timecost
        },
        function (traceObject) {
          return traceObject
        },
        "Operation Timecost",
        {title: "Cost (MS)"},
        env.isTest ? env.testTime : 0
      );
    });
  };
}

SchedulerTab.prototype = Object.create(Tab.prototype);