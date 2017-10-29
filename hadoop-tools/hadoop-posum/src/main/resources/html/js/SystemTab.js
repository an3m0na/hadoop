function SystemTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.activate = function () {
    var path = env.isTest ? "js/metrics_system.json" : self.comm.psPath + "/system";
    self.comm.requestData(path, function (data) {
      updateTimeSeries(self,
        "plot_ps_jvm",
        data,
        function (data) {
          return data.jvm
        },
        function (traceObject) {
          return traceObject
        },
        "JVM Memory for Portfolio Scheduler",
        {title: "Memory (GB)", tickmode: "linear", dtick: 0.25}
      );
      updateTimeSeries(self,
        "plot_ps_cpu",
        data,
        function (data) {
          return data.cpu
        },
        function (traceObject) {
          return traceObject
        },
        "CPU Load for Portfolio Scheduler",
        {title: "Fraction (%)", tickmode: "auto", nticks: 10},
        env.isTest ? env.testTime : 0
      );
      updateTimeSeries(self,
        "plot_ps_threads",
        data,
        function (data) {
          return {"count": data.threadCount}
        },
        function (traceObject) {
          return traceObject
        },
        "Active Threads for Portfolio Scheduler",
        {title: "Total Number", tickmode: "linear", dtick: 0.25},
        env.isTest ? env.testTime : 0
      );
    });

    path = env.isTest ? "js/metrics_system.json" : self.comm.masterPath + "/system";
    self.comm.requestData(path, function (data) {
      updateTimeSeries(self,
        "plot_pm_jvm",
        data,
        function (data) {
          return data.jvm
        },
        function (traceObject) {
          return traceObject
        },
        "JVM Memory for POSUM Master",
        {title: "Memory (GB)", tickmode: "linear", dtick: 0.25},
        env.isTest ? env.testTime : 0
      );
      updateTimeSeries(self,
        "plot_pm_cpu",
        data,
        function (data) {
          return data.cpu
        },
        function (traceObject) {
          return traceObject
        },
        "CPU Load for POSUM Master",
        {title: "Fraction (%)", tickmode: "auto", nticks: 10},
        env.isTest ? env.testTime : 0
      );
      updateTimeSeries(self,
        "plot_pm_threads",
        data,
        function (data) {
          return {"count": data.threadCount}
        },
        function (traceObject) {
          return traceObject
        },
        "Active Threads for POSUM Master",
        {title: "Total Number", tickmode: "linear", dtick: 0.25},
        env.isTest ? env.testTime : 0
      );
    });

    path = env.isTest ? "js/metrics_system.json" : self.comm.dmPath + "/system";
    self.comm.requestData(path, function (data) {
      updateTimeSeries(self,
        "plot_dm_jvm",
        data,
        function (data) {
          return data.jvm
        },
        function (traceObject) {
          return traceObject
        },
        "JVM Memory for Data Master",
        {title: "Memory (GB)", tickmode: "linear", dtick: 0.25},
        env.isTest ? env.testTime : 0
      );
      updateTimeSeries(self,
        "plot_dm_cpu",
        data,
        function (data) {
          return data.cpu
        },
        function (traceObject) {
          return traceObject
        },
        "CPU Load for Data Master",
        {title: "Fraction (%)", tickmode: "auto", nticks: 10},
        env.isTest ? env.testTime : 0
      );
      updateTimeSeries(self,
        "plot_dm_threads",
        data,
        function (data) {
          return {"count": data.threadCount}
        },
        function (traceObject) {
          return traceObject
        },
        "Active Threads for Data Master",
        {title: "Total Number", tickmode: "linear", dtick: 0.25},
        env.isTest ? env.testTime : 0
      );
    });
    self.comm.requestData(path, function (data) {
      path = env.isTest ? "js/metrics_system.json" : self.comm.smPath + "/system";
      updateTimeSeries(self,
        "plot_sm_jvm",
        data,
        function (data) {
          return data.jvm
        },
        function (traceObject) {
          return traceObject
        },
        "JVM Memory for Simulation Master",
        {title: "Memory (GB)", tickmode: "linear", dtick: 0.25},
        env.isTest ? env.testTime : 0
      );
      updateTimeSeries(self,
        "plot_sm_cpu",
        data,
        function (data) {
          return data.cpu
        },
        function (traceObject) {
          return traceObject
        },
        "CPU Load for Simulation Master",
        {title: "Fraction (%)", tickmode: "auto", nticks: 10},
        env.isTest ? env.testTime : 0
      );
      updateTimeSeries(self,
        "plot_sm_threads",
        data,
        function (data) {
          return {"count": data.threadCount}
        },
        function (traceObject) {
          return traceObject
        },
        "Active Threads for Simulation Master",
        {title: "Total Number", tickmode: "linear", dtick: 0.25},
        env.isTest ? env.testTime : 0
      );
    });
  };
}

SystemTab.prototype = Object.create(Tab.prototype);