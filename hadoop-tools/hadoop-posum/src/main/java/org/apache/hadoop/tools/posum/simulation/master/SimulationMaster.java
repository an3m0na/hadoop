package org.apache.hadoop.tools.posum.simulation.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.communication.PosumMasterProcess;
import org.apache.hadoop.tools.posum.simulation.core.SimulatorImpl;
import org.apache.hadoop.tools.posum.web.SimulatorWebApp;

public class SimulationMaster extends CompositeService implements PosumMasterProcess {

  private static final Log logger = LogFactory.getLog(SimulationMaster.class);

  private SimulatorImpl simulator;
  private SimulationMasterContext smContext;
  private SimulationMasterCommService commService;
  private SimulatorWebApp webApp;


  public SimulationMaster() {
    super(SimulationMaster.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    smContext = new SimulationMasterContext();
    smContext.setConf(conf);

    simulator = new SimulatorImpl(smContext);
    simulator.init(conf);
    addIfService(simulator);
    smContext.setSimulator(simulator);

    commService = new SimulationMasterCommService(smContext);
    commService.init(conf);
    addIfService(commService);
    smContext.setCommService(commService);

    try {
      webApp = new SimulatorWebApp(smContext,
        conf.getInt(PosumConfiguration.SIMULATOR_WEBAPP_PORT,
          PosumConfiguration.SIMULATOR_WEBAPP_PORT_DEFAULT));
    } catch (Exception e) {
      logger.error("Could not initialize web app", e);
    }

  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    if (webApp != null)
      webApp.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null)
      webApp.stop();
    super.serviceStop();
  }

  @Override
  public String getConnectAddress() {
    return commService.getConnectAddress();
  }

  public static void main(String[] args) {
    try {
      Configuration conf = PosumConfiguration.newInstance();
      SimulationMaster master = new SimulationMaster();
      master.init(conf);
      master.start();
    } catch (Exception e) {
      logger.fatal("Could not start Simulation Master", e);
    }
  }

}
