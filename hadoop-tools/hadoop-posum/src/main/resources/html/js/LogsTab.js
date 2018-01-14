function LogsTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.autoScrollOn = true;
  self.logTable = container.find("#log_table");
  self.scrollBtn = container.find("#btn_scroll");
  self.scrollBtn.bootstrapToggle().change(function () {
    self.autoScrollOn = $(this).prop('checked');
  });

  self.refresh = function () {
    self.loading = true;
    var path = env.isTest ? "mocks/logs.json" : self.comm.paths.DM + "/logs";
    self.comm.requestData(path + "?since=" + self.lastRefreshed, function (data) {
      if (data) {
        data.forEach(function (log) {
          const timestamp = moment.unix(log.timestamp / 1000);
          const sameYear = moment().subtract(1, "years").isBefore(timestamp);
          const sameDay = moment().subtract(1, "days").isBefore(timestamp);
          const timeFormat = "HH:mm:ss";
          const dateFormat = sameYear ? "MM-DD" : "YYYY-MM-DD";
          const pattern = sameDay ? timeFormat : dateFormat + " " + timeFormat;
          self.logTable.append('<tr class="info"><td class="text-nowrap">' +
            timestamp.format(pattern) + '</td><td>' + log.message.replace(/\n/g, "<br/>") +
            '</td></tr>');
        });
        if (self.autoScrollOn) {
          self.scrollToBottom();
        }
      }
      self.lastRefreshed = data[data.length - 1].timestamp;
      self.loading = false;
    }, function () {
      self.loading = false;
    });
  };

  self.scrollToBottom = function () {
    $('body,html').stop().animate({scrollTop: self.logTable[0].scrollHeight}, 1500);
  }
}

LogsTab.prototype = Object.create(Tab.prototype);