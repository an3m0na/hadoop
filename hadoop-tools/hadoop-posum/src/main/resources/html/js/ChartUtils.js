var chartColors = ["#C2F0FF", "#FFFFB2", "#D1D1FF", "#FFC2E0", "#D1FFC2", "#FFE0D1", "#B2F0E0", "#D1E0E0"];

var defaultTimeSeriesConfig = {
  listExtractor: function (data) {
    return data.entries;
  },
  entryValueExtractor: function (entry) {
    return {label: entry.value};
  },
  traceFactory: function (name) {
    return {x: [], y: [], mode: "lines", type: "scatter", name: name}
  },
  plotTitle: "Plot",
  yaxis: {title: "Value", tickmode: "auto", nticks: 10},
  xaxis: {title: "Time", type: "date"},
  baseTime: 0
};

function addNewValues(traces, config, newData) {
  $.each(config.listExtractor(newData), function (i, entry) {
    $.each(config.entryValueExtractor(entry), function (name, value) {
      var trace = traces.find(function (trace) {
        return trace.name === name;
      });
      if (!trace) {
        trace = config.traceFactory(name);
        traces.push(trace);
      }
      trace.x.push(config.baseTime + entry.time);
      trace.y.push(value);
    });
  });
  return traces;
}

function updateTimeSeriesPlot(tab,
                              element,
                              pathOrData,
                              customConfig) {
  var config = $.extend(true, {}, defaultTimeSeriesConfig, customConfig);
  var parser = function (data) {
    var plot = tab.plots[element];
    if (plot === undefined) {
      var plotConfig = {title: config.plotTitle, xaxis: config.xaxis, yaxis: config.yaxis};
      Plotly.newPlot(element, addNewValues([], config, data), plotConfig).then(function (value) {
        tab.plots[element] = value;
      });
    } else {
      addNewValues(plot.data, config, data);
      Plotly.redraw(plot);
    }
  };
  if ($.isPlainObject(pathOrData))
    parser(pathOrData);
  else
    tab.comm.requestData(pathOrData, parser);
}