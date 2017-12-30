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
      name: record.name,
      y: record.value
    };
  },
  traceFactory: function (name) {
    return {x: [], y: [], name: name, type: 'box'};
  },
  plotTitle: "Plot",
  xaxis: {title: "Trace", type: "category"},
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
        trace = defaultTimeSeriesConfig.traceFactory(name);
        trace = $.extend(true, trace, config.traceFactory(name));
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
        return trace.name === entry.name;
      });
      if (!trace) {
        trace = defaultBoxPlotConfig.traceFactory(entry.name);
        trace = $.extend(true, trace, config.traceFactory(entry.name));
        traces.push(trace);
      }
      trace.x.push(entry.x === undefined ? entry.name : entry.x);
      trace.y.push(entry.y);
    });
    var plotConfig = $.extend(true, {
      title: config.plotTitle,
      xaxis: config.xaxis,
      yaxis: config.yaxis
    }, config.layout);

    // var x = ['day 1', 'day 1', 'day 1', 'day 1', 'day 1', 'day 1',
    //   'day 2', 'day 2', 'day 2', 'day 2', 'day 2', 'day 2']
    //
    // var trace1 = {
    //   y: [0.2, 0.2, 0.6, 1.0, 0.5, 0.4, 0.2, 0.7, 0.9, 0.1, 0.5, 0.3],
    //   x: x,
    //   name: 'kale',
    //   marker: {color: '#3D9970'},
    //   type: 'box'
    // };
    //
    // var trace2 = {
    //   y: [0.6, 0.7, 0.3, 0.6, 0.0, 0.5, 0.7, 0.9, 0.5, 0.8, 0.7, 0.2],
    //   x: x,
    //   name: 'radishes',
    //   marker: {color: '#FF4136'},
    //   type: 'box'
    // };
    //
    // var trace3 = {
    //   y: [0.1, 0.3, 0.1, 0.9, 0.6, 0.6, 0.9, 1.0, 0.3, 0.6, 0.8, 0.5],
    //   x: x,
    //   name: 'carrots',
    //   marker: {color: '#FF851B'},
    //   type: 'box'
    // };
    //
    // var traces = [trace1, trace2, trace3];

    Plotly.newPlot(element, traces, plotConfig).then(function (value) {
      tab.plots[element] = value;
    });
  };
  if ($.isArray(pathOrData))
    parser(pathOrData);
  else
    tab.comm.requestData(pathOrData, parser, config.errorHandler);
}