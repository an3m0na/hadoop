function ControlsTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.initialized = false;

  self.refresh = function () {
    if (self.initialized)
      return;
    self.loading = true;
    self.comm.requestData(env.isTest ? "mocks/scale-factors.json" : self.comm.paths.OM + "/scale-factors", function (data) {
      self.updateScaleFactors(data);
      self.initialized = true;
      self.loading = false;
    }, function(){
      self.loading = false;
    });
  };

  self.updateScaleFactors = function (data) {
    $.each(data, function (name, value) {
      self.container.find("#input_" + name).val(value);
    });
  };

  container.find("#btn_save").click(function () {
    var data = {};
    self.container.find(".an3-scale-factors input").each(function () {
      var input = $(this);
      data[input.attr("id").substring("input_".length)] = input.val();
    });
    self.comm.postData(env.isTest ? "mocks/scale-factors.json" : self.comm.paths.OM + "/scale-factors", data, function (data) {
      self.comm.showDialog("Success", "The scale factors have been updated.");
      self.updateScaleFactors(data);
    });
  });
  container.find("#btn_reset").click(function () {
    self.comm.requestData(env.isTest ? "mocks/reset.json" : self.comm.paths.OM + "/reset", function () {
      self.comm.showDialog("Success", "System reset was successful.");
    });
  });
}

ControlsTab.prototype = Object.create(ControlsTab.prototype);