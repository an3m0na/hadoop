function Tab(container) {
    this.id = container.attr("id");
    this.container = container;
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
        navBar.find(".an3-nav-link").on("click", function () {
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

    self.load = function (tab) {
        if (tab.id == "scheduler") {
            var path = env.isTest ? "/html/js/scheduler_metrics.json" : env.comm.schedulerPath + "/metrics";
            env.comm.requestData(path, function (data) {
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
                var layout = {
                    title: "Policy Usage",
                    barmode: "stack",
                    xaxis: {
                        tickmode: "linear",
                        tick0: 0,
                        dtick: 10,
                        title: "Percentage of time spent running policy"
                    }
                };

                Plotly.newPlot('sch_map', chartData, layout);

                var choiceList = data.policies.list;
                var trace = {
                    x: choiceList.times,
                    y: choiceList.policies,
                    mode: "lines+markers",
                    line: {shape: "hv"},
                    type: "scatter"
                };
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

                Plotly.newPlot("sch_list", [trace], layout);
            });
        }
    };

}