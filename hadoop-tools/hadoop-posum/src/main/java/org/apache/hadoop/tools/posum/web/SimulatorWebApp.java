package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.tools.posum.simulation.master.SimulationMasterContext;

import javax.servlet.http.HttpServletRequest;

public class SimulatorWebApp extends PosumWebApp {
  private static final long serialVersionUID = 1905162041950251407L;

  private SimulationMasterContext context;

  public SimulatorWebApp(SimulationMasterContext context, int metricsAddressPort) {
    super(metricsAddressPort);
    this.context = context;
  }

  @Override
  protected JsonNode handleRoute(String route, HttpServletRequest request) {
    switch (route) {
      case "/system":
        return getSystemMetrics();
      case "/simulate":
        return simulate();
      default:
        return handleUnknownRoute();
    }
  }

  private JsonNode simulate() {
    context.getSimulator().startSimulation();
    return wrapResult("Simulation triggered");
  }

}
