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
    console.log(navItems);

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

        self.load();

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

    self.load = function (data) {
        var trace1 = {
            x: [20, 14, 23],
            y: ['giraffes', 'orangutans', 'monkeys'],
            name: 'SF Zoo',
            orientation: 'h',
            marker: {
                color: 'rgba(55,128,191,0.6)',
                width: 1
            },
            type: 'bar'
        };

        var trace2 = {
            x: [12, 18, 29],
            y: ['giraffes', 'orangutans', 'monkeys'],
            name: 'LA Zoo',
            orientation: 'h',
            type: 'bar',
            marker: {
                color: 'rgba(255,153,51,0.6)',
                width: 1
            }
        };

        var data = [trace1, trace2];

        var layout = {
            title: 'Colored Bar Chart',
            barmode: 'stack'
        };

        Plotly.newPlot('myDiv', data, layout);
    };

}