function ControlsTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.initialized = {factors: false, policy: false};
  self.policySelect = self.container.find("#input_policy");

  self.refresh = function () {
    if (self.initialized.factors && self.initialized.policy)
      return;
    self.loading = true;
    self.comm.requestData(env.isTest ? "mocks/scale-factors.json" : self.comm.paths.OM + "/scale-factors", function (data) {
      self.updateScaleFactors(data);
      self.initialized.factors = true;
      self.loading = self.initialized.policy;
    }, function () {
      self.loading = self.initialized.policy;
    });
    self.comm.requestData(env.isTest ? "mocks/policy.json" : self.comm.paths.OM + "/policy", function (data) {
      self.updatePolicy(data);
      self.initialized.policy = true;
      self.loading = self.initialized.factors;
    }, function () {
      self.loading = self.initialized.factors;
    });
  };

  self.updateScaleFactors = function (data) {
    $.each(data, function (name, value) {
      self.container.find("#input_" + name).val(value);
    });
  };

  self.updatePolicy = function (data) {
    self.policySelect.empty();
    $.each(data.options, function (i, value) {
      var option = $("<option>").append(value);
      self.policySelect.append(option);
    });
    self.policySelect.val(data.selected);
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
  container.find("#btn_switch").click(function () {
    var data = {policy: self.policySelect.val()};
    self.comm.postData(env.isTest ? "mocks/policy.json" : self.comm.paths.OM + "/policy", data, function (data) {
      self.comm.showDialog("Success", "The scheduling approach has been changed");
      self.updatePolicy(data);
    });
  });
}

ControlsTab.prototype = Object.create(ControlsTab.prototype);