function AnalysisTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.initialized = false;
  self.primaryFill = '#EBF4F3';
  self.secondaryFill = '#E1E1E1';
  self.primaryOutline = "#00736A";
  self.secondaryOutline = '#450E61';
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
              y: record["RelativeError"]
            };
          },
          plotTitle: "Task Prediction Error",
          xaxis: {title: "Time Bucket (minutes since workload start)"},
          yaxis: {title: "Relative Prediction Error"},
          traceFactory: function () {
            return {fillcolor: self.primaryFill, line: {color: self.primaryOutline}};
          }
        });
        self.createBoxPlotWithTaskType("plot_prediction_by_task_type", "Error By Task Type", data);
        self.createBoxPlotWithTaskType("plot_prediction_sort", "Error By Task Type For Sort", data.filter(function (record) {
          return record["JobType"].startsWith("SORT")
        }));
        self.createBoxPlotWithTaskType("plot_prediction_grep", "Error By Task Type For Grep", data.filter(function (record) {
          return record["JobType"].startsWith("GREP")
        }));
        self.createBoxPlotWithTaskType("plot_prediction_index", "Error By Task Type For Index", data.filter(function (record) {
          return record["JobType"] === ("INDEX")
        }));

        self.initialized = true;
        self.loading = false;
      }, function () {
        self.loading = false;
      }
    );
    self.createBoxPlotWithTaskType = function (element, title, data) {
      createBoxPlot(self, element, data, {
        entryExtractor: function (record) {
          return {
            group: record["TimeBucket"] + "-" + record["TaskType"],
            label: record["TaskType"],
            x: record["TimeBucket"],
            y: record["RelativeError"]
          };
        },
        plotTitle: title,
        xaxis: {title: "Time Bucket (minutes since workload start)"},
        yaxis: {title: "Relative Prediction Error"},
        traceFactory: function (group) {
          return {
            fillcolor: group.endsWith("MAP") ? self.primaryFill : self.secondaryFill,
            line: {color: group.endsWith("MAP") ? self.primaryOutline : self.secondaryOutline}
          };
        },
        layout: {boxmode: 'group'}
      });
    }
  };
}

AnalysisTab.prototype = Object.create(Tab.prototype);