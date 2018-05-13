function AnalysisTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.initialized = false;
  self.colors = [];
  self.colors.push('#CCCCCC');
  self.colors.push('#808080');// or #A8A8A8
  self.colors.push('#78E2D2');
  self.colors.push('#00736A');
  self.colors.push('#A0A5F2');
  self.colors.push('#5A56A3');
  self.colors.push('#FFFFB2');
  self.colors.push('#e8e847');
  self.colors.push('#a1e7fc');
  self.colors.push('#0198c6');
  self.predictionColors = {
    All: 0,
    MAP: 1,
    REDUCE: 2,
    MAP_LOCAL: 3,
    MAP_REMOTE: 4
  };
  self.refresh = function () {
    self.loading = true;
    if (self.initialized)
      return;
    self.loading = true;
    var path = env.isTest ? "mocks/task_prediction_analysis.json" : self.comm.paths.DM + "/task_prediction_analysis";
    self.comm.requestData(path, function (data) {
          var detailedMaps = data.filter(function (record) {
            return record["Predictor"] === "DETAILED" && record["TaskType"] === "MAP";
          });
          var correctedData = data.filter(function (record) {
            return record["Predictor"] !== "STANDARD" || record["TaskType"] !== "MAP";
          }).concat(detailedMaps.map(function (record) {
            return jQuery.extend(true, {}, record, {Predictor: "STANDARD"});
          }));
          self.createBoxPlotWithTaskType(correctedData, "BASIC");
          self.createBoxPlotWithTaskType(correctedData, "STANDARD");
          self.createBoxPlotWithTaskType(correctedData, "DETAILED");
          //
          self.createBoxPlotWithTaskType(correctedData, "STANDARD", "NAIVEBAYES");
          self.createBoxPlotWithTaskType(correctedData, "STANDARD", "WORDCOUNT");
          self.createBoxPlotWithTaskType(correctedData, "STANDARD", "SORT");
          self.createBoxPlotWithTaskType(correctedData, "STANDARD", "INDEX");
          self.initialized = true;
          self.loading = false;
        }, function () {
          self.loading = false;
        }
    );
    self.createBoxPlotWithTaskType = function (data, predictor, jobType) {
      var element = "plot_" + predictor + (jobType ? "_" + jobType : "");
      var title = predictor + " Predictor Errors by Task Type" + (jobType ? " for " + jobType : "");
      var filteredData = data.filter(function (record) {
        return record["TaskType"].indexOf("SKIP") < 0 && record["Predictor"] === predictor && (jobType ? record["JobType"] === jobType : true)
      });
      createBoxPlot(self, element, filteredData, {
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
          var color = self.predictionColors[name];
          return {
            // marker: {color:  name.endsWith("MAP") ? self.primaryOutline : self.secondaryOutline}
            fillcolor: self.colors[color * 2],
            line: {color: self.colors[color * 2 + 1]}
          };
        },
        layout: {boxmode: 'group', boxgroupgap: 0}
      });
    };
  };
}

AnalysisTab.prototype = Object.create(Tab.prototype);