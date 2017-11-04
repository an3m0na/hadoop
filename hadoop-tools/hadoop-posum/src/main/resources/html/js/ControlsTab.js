function ControlsTab(id, container, env) {
  Tab.call(this, id, container, env);
  var self = this;
  self.coefficientsPath = env.isTest ? "mocks/coefficients.json" : self.comm.masterPath + "/coefficients";
  self.resetPath = env.isTest ? "mocks/reset.json" : self.comm.masterPath + "/reset";
  self.activate = function () {
    self.comm.requestData(self.coefficientsPath, function (data) {
      self.updateCoefficients(data);
    });
  };

  self.updateCoefficients = function (data) {
    $.each(data, function (name, value) {
      self.container.find("#input_" + name).val(value);
    });
  };

  container.find("#btn_save").click(function () {
    var data = {};
    self.container.find(".an3-coefficients input").each(function(){
      var input = $(this);
      data[input.attr("id").substring("input_".length)] = input.val();
    });
    self.comm.postData(self.coefficientsPath, data, function (data) {
      self.comm.showDialog("Success", "The coefficients have been updated.");
      self.updateCoefficients(data);
    });
  });
  container.find("#btn_reset").click(function () {
    self.comm.requestData(self.resetPath, function (data) {
      self.comm.showDialog("Success", "System reset was successful.");
      self.updateCoefficients(data);
    });
  });
}

ControlsTab.prototype = Object.create(ControlsTab.prototype);