var chartColors = ["#C2F0FF", "#FFFFB2", "#D1D1FF", "#FFC2E0", "#D1FFC2", "#FFE0D1", "#B2F0E0", "#D1E0E0"];

function updatePlot(tab,
                    name,
                    pathOrData,
                    createPlot,
                    updateTraces) {
  var parser = function (data) {
    var plot = tab.plots[name];
    if (plot === undefined) {
      var plotAttributes = createPlot(data);
      Plotly.newPlot(name, plotAttributes.traces, plotAttributes.layout).then(function (value) {
        tab.plots[name] = value;
      });
    } else {
      updateTraces(plot.data, data);
      Plotly.redraw(plot);
    }
  };
  if ($.isPlainObject(pathOrData))
    parser(pathOrData);
  else
    tab.comm.requestData(pathOrData, parser);
}

function updateTimeSeries(tab,
                          name,
                          pathOrData,
                          getTracePoints,
                          getDataPointValue,
                          plotTitle,
                          yaxis,
                          baseTime) {
  self.updatePlot(tab, name, pathOrData, function (data) {
    var traces = [];
    $.each(getTracePoints(data), function (k, v) {
      traces.push({
        x: [data.time],
        y: [getDataPointValue(v)],
        mode: "lines",
        type: "scatter",
        name: k
      });
    });
    return {
      traces: traces,
      layout: {
        title: plotTitle,
        xaxis: {
          title: "Time",
          type: "date"
        },
        yaxis: yaxis
      }
    };
  }, function (traces, data) {
    traces.forEach(function (trace) {
      trace.x.push(data.time + baseTime);
      trace.y.push(getDataPointValue(getTracePoints(data)[trace.name]));
    });
  });
}

var defaultTimeSeriesConfig = {
  listExtractor: function (data) {
    return data.entries;
  },
  entryValueExtractor: function (entry) {
    return {label: entry.value};
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
        trace = {x: [], y: [], mode: "lines", type: "scatter", name: name};
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