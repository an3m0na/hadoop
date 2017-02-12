function Communicator(env) {
  var self = this;
  self.psPath = "http://localhost:18010/ajax";
  self.dmPath = "http://localhost:18020/ajax";
  self.smPath = "http://localhost:18030/ajax";
  self.masterPath = "/ajax";
  var generalDialog = $("#general_dialog");
  var loadingModal = $("#loading_modal");

  self.showLoading = function () {
    console.log("showing kitty");
    loadingModal.modal("show");
  };

  self.hideLoading = function () {
    console.log("hiding kitty");
    loadingModal.modal("hide");
  };

  self.closeDialog = function () {
    generalDialog.modal("hide");
  };

  self.showDialog = function (title, text, closeFunction, saveFunction) {
    generalDialog.find(".an3-modal-title").text(title);
    generalDialog.find(".an3-modal-text").text(text);
    var closeButton = generalDialog.find(".an3-modal-close");
    closeButton.off("click");
    if (closeFunction)
      closeButton.on("click", closeFunction);
    var saveButton = generalDialog.find(".an3-modal-save");
    if (!saveFunction)
      saveButton.hide();
    else {
      saveButton.show().off("click").on("click", saveFunction);
    }
    generalDialog.modal();
  };

  $["postJSON"] = function (url, data, callback) {
    // shift arguments if data argument was omitted
    if ($.isFunction(data)) {
      callback = data;
      data = undefined;
    }

    return $.ajax({
      url: url,
      type: "POST",
      contentType: "application/json; charset=utf-8",
      dataType: "json",
      data: JSON.stringify(data),
      success: callback
    });
  };

  self.handleServerResponse = function (response, success, fail) {
    if (typeof response === "string") {
      response = JSON.parse(response);
    }
    if (response.successful) {
      console.log("Received data");
      console.log(response.result);
      console.log("---");
      success(response.result);
    }
    else {
      self.handleServerError(null, response, fail);
    }
  };

  self.handleServerError = function (jqXHR, result, fail) {
    console.log(result);
    console.log("---");
    var parsedResult = null;

    if (typeof result === "string") {
      try {
        parsedResult = JSON.parse(result);
      } catch (e) {
        console.log("Error result is not a JSON.");
      }
    } else {
      parsedResult = result;
    }

    var errorObject = {
      isGeneral: jqXHR ? true : false,
      code: jqXHR ? jqXHR.status : parsedResult.result.code,
      message: parsedResult ? parsedResult.result.message : result
    };

    if (fail)
      fail(errorObject);
    else {
      self.showDialog("Error", "Error occurred:\n" +
        errorObject.message +
        "\n\nPlease fix the error and try again.");
    }
  };

  function generalRequest(isPost, isRaw, showData, path, data, success, fail) {
    console.log("---");
    console.log("Sending " + (isPost ? "POST" : "GET") + " request to " + path + "...");
    if (isPost && showData)
      console.log(data);
    var ajaxMethod = isPost ? (isRaw ? $.post : $.postJSON) : (isRaw ? $.get : $.getJSON);
    (isPost ? ajaxMethod(path, data) : ajaxMethod(path))
      .success(function (response) {
        if (isRaw && !isPost) {
          success(response);
          return;
        }
        self.handleServerResponse(response, success, fail);
      })
      .fail(function (jqXHR, textStatus) {
        self.handleServerError(jqXHR, textStatus, fail);
      });
  }

  self.requestData = function (path, success, fail) {
    generalRequest(false, false, true, path, null, success, fail);
  };

  self.postData = function (path, data, success, fail) {
    generalRequest(true, false, true, path, data, success, fail);
  };

  self.requestRawData = function (path, success, fail) {
    generalRequest(false, true, false, path, null, success, fail);
  };

  self.postRawData = function (path, data, success, fail) {
    generalRequest(true, true, false, path, data, success, fail);
  };

  self.initialize = function () {
    return self;
  }


}