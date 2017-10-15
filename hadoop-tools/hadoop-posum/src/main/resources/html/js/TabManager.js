function Tab(container) {
  this.id = container.attr("id");
  this.container = container;
  this.plots = {};
  this.lastRefreshed = 0;
}

function TabManager(env) {
  var self = this;
  var tabContainers = $(".an3-tab");
  var tabs = {};
  var navBar = $("#navbar");
  var navItems = navBar.find("li");
  var colors = ["#C2F0FF", "#FFFFB2", "#D1D1FF", "#FFC2E0", "#D1FFC2", "#FFE0D1", "#B2F0E0", "#D1E0E0"];

  self.switchToTab = function (name) {
    tabContainers.hide();
    var tab = tabs[name];
    if (!tab)
      tabs.none.container.show();
    else {
      tab.container.show();
    }
  };

  self.initialize = function () {
    $(".an3-nav-link").on("click", function () {
        var link = $(this);
        var div = link.attr("href");
        var newState = div.substr(1);
        self.changeState(newState, link.attr("id") === "btn_start" ? null : link);
      }
    );

    tabContainers.each(function (i, e) {
      var tab = new Tab($(e));
      tabs[tab.id] = tab;
    });

    setInterval(function () {
      env.testTime += env.refreshInterval;
      self.load(tabs[env.state]);
    }, env.refreshInterval);

    return self;
  };

  self.changeState = function (newState, link) {
    navItems.removeClass("active");
    if (!link)
      link = navItems.find(".an3-nav-link[href=#" + newState + "]");
    link.parent().addClass("active");

    env.state = newState;
    self.switchToTab(newState);
  };

  self.time = 0;
  self.load = function (tab) {
    var path, traces, layout;
    if (tab.id === "scheduler") {
      path = env.isTest ? "js/dmmetrics_policies.json" : env.comm.dmPath + "/policies";
      env.comm.requestData(path, function (data) {

        //plot_policies_map
        var totalTime = 0;
        var chartData = [];
        var crtColor = 0;
        $.each(data.policies.map, function (k, v) {
          totalTime += v.time
        });
        $.each(data.policies.map, function (k, v) {
          var trace = {
            x: [Math.round(v.time / totalTime * 100)],
            y: ["Policy"],
            name: k,
            orientation: "h",
            marker: {
              color: colors[crtColor++],
              width: 1
            },
            type: "bar"
          };
          chartData.push(trace);
        });
        layout = {
          title: "Policy Usage",
          barmode: "stack",
          xaxis: {
            tickmode: "linear",
            tick0: 0,
            dtick: 10,
            title: "Percentage of time spent running policy"
          }
        };
        Plotly.newPlot('plot_policies_map', chartData, layout);

        //plot_policies_list
        var choiceList = data.policies.list;
        traces = [{
          x: choiceList.times,
          y: choiceList.policies,
          mode: "lines+markers",
          line: {shape: "hv"},
          type: "scatter"
        }];
        layout = {
          title: "Policy Choices",
          xaxis: {
            title: "Time",
            type: "date"
          },
          yaxis: {
            title: "Policy"
          }
        };
        Plotly.newPlot("plot_policies_list", traces, layout);
      });

      path = env.isTest ? "js/psmetrics_scheduler.json" : env.comm.psPath + "/scheduler";
      env.comm.requestData(path, function (data) {
        self.updateTimeSeries(tab,
          "plot_timecost",
          data,
          function (data) {
            return data.timecost
          },
          function (traceObject) {
            return traceObject
          },
          "Operation Timecost",
          {title: "Cost (MS)"}
        );
      });
    } else if (tab.id === "system") {
      path = env.isTest ? "js/metrics_system.json" : env.comm.psPath + "/system";
      self.updateTimeSeries(tab,
        "plot_ps_jvm",
        path,
        function (data) {
          return data.jvm
        },
        function (traceObject) {
          return traceObject
        },
        "JVM Memory for Portfolio Scheduler",
        {title: "Memory (GB)", tickmode: "linear", dtick: 0.25}
      );
      self.updateTimeSeries(tab,
        "plot_ps_cpu",
        path,
        function (data) {
          return data.cpu
        },
        function (traceObject) {
          return traceObject
        },
        "CPU Load for Portfolio Scheduler",
        {title: "Fraction (%)", tickmode: "auto", nticks: 10}
      );
      self.updateTimeSeries(tab,
        "plot_ps_threads",
        path,
        function (data) {
          return {"count": data.threadCount}
        },
        function (traceObject) {
          return traceObject
        },
        "Active Threads for Portfolio Scheduler",
        {title: "Total Number", tickmode: "linear", dtick: 0.25}
      );
      path = env.isTest ? "js/metrics_system.json" : env.comm.masterPath + "/system";
      self.updateTimeSeries(tab,
        "plot_pm_jvm",
        path,
        function (data) {
          return data.jvm
        },
        function (traceObject) {
          return traceObject
        },
        "JVM Memory for POSUM Master",
        {title: "Memory (GB)", tickmode: "linear", dtick: 0.25}
      );
      self.updateTimeSeries(tab,
        "plot_pm_cpu",
        path,
        function (data) {
          return data.cpu
        },
        function (traceObject) {
          return traceObject
        },
        "CPU Load for POSUM Master",
        {title: "Fraction (%)", tickmode: "auto", nticks: 10}
      );
      self.updateTimeSeries(tab,
        "plot_pm_threads",
        path,
        function (data) {
          return {"count": data.threadCount}
        },
        function (traceObject) {
          return traceObject
        },
        "Active Threads for POSUM Master",
        {title: "Total Number", tickmode: "linear", dtick: 0.25}
      );
      path = env.isTest ? "js/metrics_system.json" : env.comm.dmPath + "/system";
      self.updateTimeSeries(tab,
        "plot_dm_jvm",
        path,
        function (data) {
          return data.jvm
        },
        function (traceObject) {
          return traceObject
        },
        "JVM Memory for Data Master",
        {title: "Memory (GB)", tickmode: "linear", dtick: 0.25}
      );
      self.updateTimeSeries(tab,
        "plot_dm_cpu",
        path,
        function (data) {
          return data.cpu
        },
        function (traceObject) {
          return traceObject
        },
        "CPU Load for Data Master",
        {title: "Fraction (%)", tickmode: "auto", nticks: 10}
      );
      self.updateTimeSeries(tab,
        "plot_dm_threads",
        path,
        function (data) {
          return {"count": data.threadCount}
        },
        function (traceObject) {
          return traceObject
        },
        "Active Threads for Data Master",
        {title: "Total Number", tickmode: "linear", dtick: 0.25}
      );
      path = env.isTest ? "js/metrics_system.json" : env.comm.smPath + "/system";
      self.updateTimeSeries(tab,
        "plot_sm_jvm",
        path,
        function (data) {
          return data.jvm
        },
        function (traceObject) {
          return traceObject
        },
        "JVM Memory for Simulation Master",
        {title: "Memory (GB)", tickmode: "linear", dtick: 0.25}
      );
      self.updateTimeSeries(tab,
        "plot_sm_cpu",
        path,
        function (data) {
          return data.cpu
        },
        function (traceObject) {
          return traceObject
        },
        "CPU Load for Simulation Master",
        {title: "Fraction (%)", tickmode: "auto", nticks: 10}
      );
      self.updateTimeSeries(tab,
        "plot_sm_threads",
        path,
        function (data) {
          return {"count": data.threadCount}
        },
        function (traceObject) {
          return traceObject
        },
        "Active Threads for Simulation Master",
        {title: "Total Number", tickmode: "linear", dtick: 0.25}
      );
    } else if (tab.id === "cluster") {
      path = env.isTest ? "js/psmetrics_cluster.json" : env.comm.psPath + "/cluster";
      env.comm.requestData(path, function (data) {

        self.updateTimeSeries(tab,
          "plot_apps",
          data,
          function (data) {
            return data.running
          },
          function (traceObject) {
            return traceObject.applications
          },
          "Running Applications",
          {title: "Number", tickmode: "linear"}
        );

        self.updateTimeSeries(tab,
          "plot_containers",
          data,
          function (data) {
            return data.running
          },
          function (traceObject) {
            return traceObject.containers
          },
          "Running Containers",
          {title: "Number", tickmode: "linear"}
        );
      });
    } else if (tab.id === "logs") {
      path = env.isTest ? "js/logs.json" : env.comm.dmPath + "/logs";
      const lastRefreshed = tab.lastRefreshed;
      env.comm.requestData(path + "?since=" + lastRefreshed, function (data) {
        if (!data || data.length === 0)
          return;
        data.forEach(function (log) {
          const timestamp = moment.unix(log.timestamp / 1000);
          const sameYear = moment().subtract(1, "years").isBefore(timestamp);
          const sameDay = moment().subtract(1, "days").isBefore(timestamp);
          const timeFormat = "HH:mm:ss";
          const dateFormat =  sameYear? "MM-DD" : "YYYY-MM-DD";
          const pattern = sameDay? timeFormat : dateFormat + " " + timeFormat;
          tab.container.find("#log_table")
            .append('<tr class="info"><td class="text-nowrap">' +
              timestamp.format(pattern) + '</td><td>' + log.message.replace(/\n/g, "<br/>") +
              '</td></tr>');
        });
        tab.lastRefreshed = data[data.length - 1].timestamp;
      });
    }
  };

  self.updatePlot = function (tab, name, pathOrData, createPlot, updateTraces) {
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
      env.comm.requestData(pathOrData, parser);
  };

  self.updateTimeSeries = function (tab,
                                    name,
                                    pathOrData,
                                    getTracePoints,
                                    getDataPointValue,
                                    plotTitle,
                                    yaxis) {
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
        trace.x.push(data.time + (env.isTest ? env.testTime : 0));
        trace.y.push(getDataPointValue(getTracePoints(data)[trace.name]));
      });
    });
  }

}