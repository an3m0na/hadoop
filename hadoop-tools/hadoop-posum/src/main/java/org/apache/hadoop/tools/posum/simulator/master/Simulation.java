package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

import java.util.concurrent.Callable;


public class Simulation implements Callable<SimulationResultPayload> {
    private static final Log logger = LogFactory.getLog(SimulationMaster.class);

    private volatile boolean exit = false;
    private String policy;
    private JobBehaviorPredictor predictor;
    private DataStore dataStore;
    private DataEntityDB dbReference;
    private SimulationStatistics stats;

    public Simulation(JobBehaviorPredictor predictor, String policy, DataStore dataStore) {
        this.predictor = predictor;
        this.policy = policy;
        this.dataStore = dataStore;
        this.stats = new SimulationStatistics();
    }

    private void setUp() {
        dbReference = DataEntityDB.get(DataEntityDB.Type.SIMULATION, policy);
        dataStore.clearDatabase(dbReference);
        dataStore.copyDatabase(DataEntityDB.getSimulation(), dbReference);
        Database db = Database.extractFrom(dataStore, dbReference);
        predictor.initialize(db);
        //TODO setStartTimeCluster (once lastUpdated is in place)
        stats.setStartTimePhysical(System.currentTimeMillis());
    }

    private void tearDown() {
        //TODO setEndTimeCluster
        stats.setEndTimePhysical(System.currentTimeMillis());
        dataStore.clearDatabase(dbReference);
        //TODO log stats
    }

    @Override
    public SimulationResultPayload call() throws Exception {
        setUp();
        try {
            Thread.sleep(5000);
            return SimulationResultPayload.newInstance(policy,
                    CompoundScorePayload.newInstance(Math.random() * 10, 0.0, 0.0));
        } catch (Exception e) {
            logger.error("Error during simulation. Shutting down simulation...", e);
            return SimulationResultPayload.newInstance(policy, CompoundScorePayload.newInstance(0.0, 0.0, 0.0));
        } finally {
            tearDown();
        }
    }
}
