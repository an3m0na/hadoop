package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.simulation.master.SimulationMaster;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;

import java.util.List;
import java.util.concurrent.Callable;


public class Simulation implements Callable<SimulationResultPayload> {
    private static final Log logger = LogFactory.getLog(SimulationMaster.class);

    private volatile boolean exit = false;
    private String policy;
    private JobBehaviorPredictor predictor;
    private DataStore dataStore;
    private DatabaseReference dbReference;
    private SimulationStatistics stats;
    private static final FindByQueryCall GET_LATEST =
            FindByQueryCall.newInstance(DataEntityCollection.JOB, null, "lastUpdated", true, 0, 1);


    public Simulation(JobBehaviorPredictor predictor, String policy, DataStore dataStore) {
        this.predictor = predictor;
        this.policy = policy;
        this.dataStore = dataStore;
        this.stats = new SimulationStatistics();
    }

    private void setUp() {
        dbReference = DatabaseReference.get(DatabaseReference.Type.SIMULATION, policy);
        dataStore.clearDatabase(dbReference);
        dataStore.copyDatabase(DatabaseReference.getSimulation(), dbReference);
        Database db = Database.extractFrom(dataStore, dbReference);
        predictor.initialize(db);
        stats.setStartTimeCluster(getLastUpdated());
        stats.setStartTimePhysical(System.currentTimeMillis());
    }

    private void tearDown() {
        stats.setEndTimeCluster(getLastUpdated());
        stats.setEndTimePhysical(System.currentTimeMillis());
        dataStore.clearDatabase(dbReference);
        //TODO log stats
    }

    private Long getLastUpdated(){
        List<JobProfile> latest = dataStore.executeDatabaseCall(GET_LATEST, dbReference).getEntities();
        if(latest != null && latest.size() > 0)
            return latest.get(0).getLastUpdated();
        return null;
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
