package org.apache.hadoop.tools.posum.web;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.simulation.master.SimulationMasterContext;

public class SimulatorWebApp extends PosumWebApp {
  private static final long serialVersionUID = 1905162041950251407L;
  private static Log logger = LogFactory.getLog(SimulatorWebApp.class);

  private SimulationMasterContext context;

  public SimulatorWebApp(SimulationMasterContext context, int metricsAddressPort) {
    super(metricsAddressPort);
    this.context = context;
  }

}
