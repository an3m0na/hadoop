function AnalysisTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.initialized = false;

  self.refresh = function () {
    self.loading = true;
    if (self.initialized)
      return;
    self.loading = true;
    var path = env.isTest ? "mocks/task_prediction_analysis.json" : self.comm.paths.DM + "/task_prediction_analysis";
    self.comm.requestData(path, function (data) {
      var traces = [];
      var traceFactory = function (name) {
        return {x: [], y: [], name: name, type: 'box'};
      };

      $.each(data, function (index, record) {
        var name = record["TimeBucket"];
        var trace = traces.find(function (trace) {
          return trace.name === name;
        });
        if (!trace) {
          trace = traceFactory(name);
          traces.push(trace);
        }
        trace.x.push(name);
        trace.y.push(record["RelativeError"]);
      });

      var plotConfig = {
        title: "Task Prediction Error",
        xaxis: {title: "Time Bucket (minutes since workload start) ", type: "linear"},
        yaxis: {title: "Relative Prediction Error", tickmode: "auto"},
        height: 700
      };

      var element = "plot_prediction";
      Plotly.newPlot(element, traces, plotConfig).then(function (value) {
        self.plots[element] = value;
      });
      self.initialized = true;
      self.loading = false;
    }, function () {
      self.loading = false;
    });

  };
}

AnalysisTab.prototype = Object.create(Tab.prototype);