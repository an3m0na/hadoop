package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.hadoop.tools.posum.common.util.json.JsonArray;
import org.apache.hadoop.tools.posum.common.util.json.JsonElement;
import org.apache.hadoop.tools.posum.common.util.json.JsonObject;
import org.apache.hadoop.tools.posum.orchestration.core.PosumEvent;
import org.apache.hadoop.tools.posum.orchestration.core.PosumEventType;
import org.apache.hadoop.tools.posum.orchestration.core.SimulationScoreComparator;
import org.apache.hadoop.tools.posum.orchestration.master.OrchestrationMasterContext;

import javax.servlet.http.HttpServletRequest;

public class OrchestratorWebApp extends PosumWebApp {
  public static final String DYNAMIC = "DYNAMIC";
  private OrchestrationMasterContext context;

  public OrchestratorWebApp(OrchestrationMasterContext context, int metricsAddressPort) {
    super(metricsAddressPort);
    this.context = context;
    staticHandler.setWelcomeFiles(new String[]{"posumstats.html"});
  }

  @Override
  protected JsonNode handleRoute(String route, HttpServletRequest request) {
    switch (route) {
      case "/conf":
        return getConfiguration();
      case "/system":
        return getSystemMetrics();
      case "/scale-factors":
        if (request.getMethod().equals("POST")) {
          return updateScaleFactors(readPostedObject(request));
        }
        return getScaleFactors();
      case "/reset":
        return reset();
      case "/policy":
        if (request.getMethod().equals("POST")) {
          return switchPolicy(readPostedObject(request).get("policy").getNode().asText());
        }
        return getPolicyOptions();
      default:
        return handleUnknownRoute();
    }
  }

  private JsonNode getPolicyOptions() {
    JsonElement options = JsonElement.write(context.getPolicyPortfolio().keySet());
    return wrapResult(new JsonObject()
      .put("options", new JsonArray((ArrayNode) options.getNode()).add(DYNAMIC))
      .put("selected", context.isSwitchEnabled() ? DYNAMIC : context.getCurrentPolicy())
      .getNode());
  }

  private JsonNode switchPolicy(String policy) {
    if (DYNAMIC.equals(policy)) {
      context.setSwitchEnabled(true);
    } else if (context.getPolicyPortfolio().containsKey(policy)) {
      context.setSwitchEnabled(false);
      context.getCommService().getScheduler().changeToPolicy(policy);
      context.setCurrentPolicy(policy);
    } else {
      return wrapError("INCORRECT_POLICY", "Provided policy is not supported", null);
    }
    return getPolicyOptions();
  }

  private JsonNode updateScaleFactors(JsonObject input) {
    context.getSimulationScoreComparator().updateScaleFactors(
      input.getNumber("alpha"),
      input.getNumber("beta"),
      input.getNumber("gamma"));
    return getScaleFactors();
  }

  private JsonNode getScaleFactors() {
    SimulationScoreComparator comparator = context.getSimulationScoreComparator();
    return wrapResult(new JsonObject()
      .put("alpha", comparator.getSlowdownScaleFactor())
      .put("beta", comparator.getPenaltyScaleFactor())
      .put("gamma", comparator.getCostScaleFactor())
      .getNode());
  }

  private JsonNode reset() {
    context.getDispatcher().getEventHandler().handle(new PosumEvent(PosumEventType.SYSTEM_RESET));
    return wrapResult("Reset triggered");
  }

  private JsonNode getConfiguration() {
    return wrapResult(new JsonObject()
      .put("addresses", new JsonObject()
        .put("DM", context.getCommService().getDMAddress())
        .put("PS", context.getCommService().getPSAddress())
        .put("SM", context.getCommService().getSMAddress())
      )
      .getNode());
  }
}
