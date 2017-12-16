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
  layout: {},
  baseTime: 0,
  errorHandler: function () {
    // ignore errors
  }
};

var defaultBoxPlotConfig = {
  entryExtractor: function (record) {
    return {
      group: record.group,
      value: record.value
    };
  },
  traceFactory: function (name) {
    return {x: [], y: [], name: name, type: 'box'};
  },
  plotTitle: "Plot",
  xaxis: {title: "Trace", type: "linear"},
  yaxis: {title: "Value", tickmode: "auto"},
  layout: {height: 700},
  errorHandler: undefined
};

function addNewTicks(traces, config, newData) {
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
      var plotConfig = $.extend(true, {
        title: config.plotTitle,
        xaxis: config.xaxis,
        yaxis: config.yaxis
      }, config.layout);
      Plotly.newPlot(element, addNewTicks([], config, data), plotConfig).then(function (value) {
        tab.plots[element] = value;
      });
    } else {
      addNewTicks(plot.data, config, data);
      Plotly.redraw(plot);
    }
  };
  if ($.isPlainObject(pathOrData))
    parser(pathOrData);
  else
    tab.comm.requestData(pathOrData, parser, config.errorHandler);
}

function createBoxPlot(tab,
                       element,
                       pathOrData,
                       customConfig) {
  var config = $.extend(true, {}, defaultBoxPlotConfig, customConfig);
  var parser = function (data) {
    var traces = [];
    $.each(data, function (index, record) {
      var entry = config.entryExtractor(record);
      var trace = traces.find(function (trace) {
        return trace.name === entry.group;
      });
      if (!trace) {
        trace = config.traceFactory(entry.group);
        traces.push(trace);
      }
      trace.x.push(entry.group);
      trace.y.push(entry.value);
    });
    var plotConfig = $.extend(true, {
      title: config.plotTitle,
      xaxis: config.xaxis,
      yaxis: config.yaxis
    }, config.layout);
    Plotly.newPlot(element, traces, plotConfig).then(function (value) {
      tab.plots[element] = value;
    });
  };
  if ($.isArray(pathOrData))
    parser(pathOrData);
  else
    tab.comm.requestData(pathOrData, parser, config.errorHandler);
}