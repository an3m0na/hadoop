package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

/**
 * Created by ane on 4/20/16.
 */
public class Simulation extends Thread {
    private static final Log logger = LogFactory.getLog(SimulationMaster.class);

    private volatile boolean exit = false;
    private SimulatorImpl simulator;
    private String policy;
    private JobBehaviorPredictor predictor;
    private DataMasterClient dataClient;


    public Simulation(SimulatorImpl simulator, String policy, SimulationMasterContext context) {
        this.simulator = simulator;
        this.policy = policy;
        this.predictor = JobBehaviorPredictor.newInstance(context.getConf());
        this.dataClient = context.getCommService().getDataClient();
        //TODO set up simulation
        predictor.initialize(dataClient.bindTo(DataEntityDB.get(DataEntityDB.Type.SIMULATION, policy)));
    }

    void exit() {
        exit = true;
        interrupt();
    }

    @Override
    public void run() {
        //TODO actual code
        try {
            sleep(5000);
            simulator.simulationDone(SimulationResultPayload.newInstance(policy,
                    CompoundScorePayload.newInstance(Math.random() * 10, 0.0, 0.0)));
        } catch (InterruptedException e) {
            logger.warn(e);
        } catch (Exception e) {
            logger.error("Error during simulation. Shutting down simulation...", e);
        }
        simulator.simulationDone(SimulationResultPayload.newInstance(policy,
                CompoundScorePayload.newInstance(0.0, 0.0, 0.0)));
    }
}
