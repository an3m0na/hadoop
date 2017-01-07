package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.MultiEntityPayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.simulation.master.SimulationMaster;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.hadoop.tools.posum.simulation.core.SimulationEvent.Type.TASK_FINISHED;


class Simulation implements Callable<SimulationResultPayload> {
    private static final Log logger = LogFactory.getLog(SimulationMaster.class);

    private volatile boolean exit = false;
    private String policy;
    private JobBehaviorPredictor predictor;
    private DataStore dataStore;
    private DatabaseReference dbReference;
    private Database db;
    private SimulationStatistics stats;
    private static final FindByQueryCall GET_LATEST =
            FindByQueryCall.newInstance(DataEntityCollection.JOB, null, "lastUpdated", true, 0, 1);
    private Queue<SimulationEvent> eventQueue;
    private Long clusterTime = 0L;
    private Integer pendingJobs = 0;
    private Double runtime = 0.0;
    private Double penalty = 0.0;
    private Double cost = 0.0;


    Simulation(JobBehaviorPredictor predictor, String policy, DataStore dataStore) {
        this.predictor = predictor;
        this.policy = policy;
        this.dataStore = dataStore;
        this.stats = new SimulationStatistics();
        this.eventQueue = new ConcurrentLinkedQueue<>();
    }

    private void setUp() {
        dbReference = DatabaseReference.get(DatabaseReference.Type.SIMULATION, policy);
        dataStore.clearDatabase(dbReference);
        dataStore.copyDatabase(DatabaseReference.getSimulation(), dbReference);
        db = Database.extractFrom(dataStore, dbReference);
        predictor.initialize(db);
        MultiEntityPayload latest = dataStore.executeDatabaseCall(GET_LATEST, dbReference);
        if (latest != null) {
            clusterTime = latest.getEntities().get(0).getLastUpdated();
            stats.setStartTimeCluster(clusterTime);
        }
        stats.setStartTimePhysical(System.currentTimeMillis());
        IdsByQueryCall getPendingJobs =
                IdsByQueryCall.newInstance(DataEntityCollection.JOB, QueryUtils.is("finishTime", 0L));
        pendingJobs = db.executeDatabaseCall(getPendingJobs).getEntries().size();
    }

    private void tearDown() {
        stats.setEndTimeCluster(clusterTime);
        stats.setEndTimePhysical(System.currentTimeMillis());
        dataStore.clearDatabase(dbReference);
        //TODO log stats
    }

    @Override
    public SimulationResultPayload call() throws Exception {
        setUp();
        try {
            initializeDaemonSimulators();
            predictBehavior();
            processQueue();
            return SimulationResultPayload.newInstance(policy, CompoundScorePayload.newInstance(runtime, penalty, cost));
        } catch (Exception e) {
            logger.error("Error during simulation. Shutting down simulation...", e);
            return SimulationResultPayload.newInstance(policy, CompoundScorePayload.newInstance(0.0, 0.0, 0.0));
        } finally {
            tearDown();
        }
    }

    private void initializeDaemonSimulators() {
        // TODO initialize the state of the hadoop daemon simulators (ResourceManager, ApplicationMasters and NodeManagers)
    }

    private void predictBehavior() {
        IdsByQueryCall getAllTasks = IdsByQueryCall.newInstance(DataEntityCollection.TASK, null);
        List<String> taskIds = dataStore.executeDatabaseCall(getAllTasks, DatabaseReference.getMain()).getEntries();
        for (String taskId : taskIds) {
            Long duration = predictor.predictTaskDuration(taskId);
            eventQueue.add(new SimulationEvent<>(TASK_FINISHED, clusterTime + duration, new TaskFinishedDetails(taskId)));
        }
    }

    private void processQueue() {
        FindByIdCall getTask = FindByIdCall.newInstance(DataEntityCollection.TASK, null);
        FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB, null);
        while (!exit) {
            SimulationEvent event = eventQueue.poll();
            clusterTime = event.getTimestamp();
            switch (event.getType()) {
                case TASK_FINISHED:
                    getTask.setId(((TaskFinishedDetails) event.getDetails()).getTaskId());
                    TaskProfile task = db.executeDatabaseCall(getTask).getEntity();
                    task.setFinishTime(clusterTime);
                    // update other task details

                    JobProfile job = db.executeDatabaseCall(getJob).getEntity();
                    if (checkLastTask(task, job)) {
                        pendingJobs--;
                        job.setFinishTime(clusterTime);
                        // update other job details
                        //TODO correctly update metrics
                        runtime = Math.random() * 10;
                        penalty++;
                        cost++;
                        if (pendingJobs == 0) {
                            exit = true;
                        }
                    }
                    // TODO update daemon simulators about task + job (job event is necessary?)
                    break;
            }
        }
    }

    private boolean checkLastTask(TaskProfile task, JobProfile job) {
        switch (task.getType()) {
            case REDUCE:
                return job.getTotalReduceTasks() - job.getCompletedReduces() == 1;
            default:
                return job.getTotalMapTasks() - job.getCompletedMaps() == 1;
        }
    }

}
