function AnalysisTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.initialized = false;
  self.primaryFill = '#78E2D2';
  self.secondaryFill = '#A0A5F2';
  self.primaryOutline = "#00736A";
  self.secondaryOutline = '#5A56A3';
  self.refresh = function () {
    self.loading = true;
    if (self.initialized)
      return;
    self.loading = true;
    var path = env.isTest ? "mocks/task_prediction_analysis.json" : self.comm.paths.DM + "/task_prediction_analysis";
    self.comm.requestData(path, function (data) {
        self.createPredictorPlots("BASIC", data);
        self.createPredictorPlots("STANDARD", data);
        self.createPredictorPlots("DETAILED", data);
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
            name: record["TaskType"],
            x: record["TimeBucket"],
            y: record["RelativeError"]
          };
        },
        plotTitle: title,
        xaxis: {title: "Time Bucket (minutes since workload start)"},
        yaxis: {title: "Relative Prediction Error"},
        traceFactory: function (name) {
          return {
            // marker: {color:  name.endsWith("MAP") ? self.primaryOutline : self.secondaryOutline}
            fillcolor: name.endsWith("MAP") ? self.primaryFill : self.secondaryFill,
            line: {color: name.endsWith("MAP") ? self.primaryOutline : self.secondaryOutline}
          };
        },
        layout: {boxmode: 'group', boxgroupgap: 0}
      });
    };
    self.createPredictorPlots = function (predictorName, data) {
      var label = predictorName[0].toUpperCase() + predictorName.substr(1).toLowerCase() + " Predictor: ";
      var predictorData = data.filter(function (record) {
        return record["Predictor"] === predictorName
      });
      createBoxPlot(self, "plot_" + predictorName, predictorData, {
        entryExtractor: function (record) {
          return {
            name: "All tasks",
            x: record["TimeBucket"],
            y: record["RelativeError"]
          };
        },
        plotTitle: label + "Task Prediction Error",
        xaxis: {title: "Time Bucket (minutes since workload start)"},
        yaxis: {title: "Relative Prediction Error"},
        traceFactory: function () {
          return {fillcolor: self.primaryFill, line: {color: self.primaryOutline}};
        }
      });
      self.createBoxPlotWithTaskType("plot_" + predictorName + "_by_task_type", label + "Error By Task Type", predictorData);
      self.createBoxPlotWithTaskType("plot_" + predictorName + "_sort", label + "Error By Task Type For Sort", predictorData.filter(function (record) {
        return record["JobType"] === "SORT"
      }));
      self.createBoxPlotWithTaskType("plot_" + predictorName + "_wordcount", label + "Error By Task Type For Wordcount", predictorData.filter(function (record) {
        return record["JobType"] === "WORDCOUNT"
      }));
      self.createBoxPlotWithTaskType("plot_" + predictorName + "_index", label + "Error By Task Type For Index", predictorData.filter(function (record) {
        return record["JobType"] === "INDEX"
      }));
      self.createBoxPlotWithTaskType("plot_" + predictorName + "_bayes", label + "Error By Task Type For Naive Bayes", predictorData.filter(function (record) {
        return record["JobType"] === "NAIVEBAYES"
      }));
    }
  };
}

AnalysisTab.prototype = Object.create(Tab.prototype);