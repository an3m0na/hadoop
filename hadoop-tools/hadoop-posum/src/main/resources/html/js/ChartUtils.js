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