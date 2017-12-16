function AnalysisTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.initialized = false;
  self.defaultPlotConfig = {
    title: "Statistics",
    xaxis: {title: "Trace", type: "linear"},
    yaxis: {title: "Value", tickmode: "auto"},
    height: 700
  };
  self.defaultTraceFactory = function (name) {
    return {x: [], y: [], name: name, type: 'box'};
  };
  self.createBoxPlot = function (element, data, groupExtractor, entryValueExtractor, customConfig) {
    var traces = [];
    $.each(data, function (index, record) {
      var name = groupExtractor(record);
      var trace = traces.find(function (trace) {
        return trace.name === name;
      });
      if (!trace) {
        trace = traceFactory(name);
        traces.push(trace);
      }
      trace.x.push(name);
      trace.y.push(entryValueExtractor(record));
    });
    Plotly.newPlot(element, traces, $.extend(true, {}, self.defaultPlotConfig, customConfig)).then(function (value) {
      self.plots[element] = value;
    });
  };
  self.refresh = function () {
    self.loading = true;
    if (self.initialized)
      return;
    self.loading = true;
    var path = env.isTest ? "mocks/task_prediction_analysis.json" : self.comm.paths.DM + "/task_prediction_analysis";
    self.comm.requestData(path, function (data) {
      createBoxPlot(self, "plot_prediction", data, {
        entryExtractor: function (record) {
          return {
            group: record["TimeBucket"],
            value: record["RelativeError"]
          };
        },
        plotTitle: "Task Prediction Error",
        xaxis: {title: "Time Bucket (minutes since workload start)"},
        yaxis: {title: "Relative Prediction Error"}
      });
      self.initialized = true;
      self.loading = false;
    }, function () {
      self.loading = false;
    });

  };
}

AnalysisTab.prototype = Object.create(Tab.prototype);