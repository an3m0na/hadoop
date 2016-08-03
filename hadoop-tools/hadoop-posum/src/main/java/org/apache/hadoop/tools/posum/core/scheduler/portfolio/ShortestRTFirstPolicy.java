package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.core.scheduler.meta.MetaSchedulerCommService;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.extca.ExtCaSchedulerNode;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.extca.ExtensibleCapacityScheduler;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by ane on 1/22/16.
 */
public class ShortestRTFirstPolicy extends ExtensibleCapacityScheduler<SRTFAppAttempt, ExtCaSchedulerNode> {


    private static Log logger = LogFactory.getLog(ShortestRTFirstPolicy.class);

    private long lastCheck = 0;
    private long maxCheck;

    public ShortestRTFirstPolicy() {
        super(SRTFAppAttempt.class, ExtCaSchedulerNode.class, ShortestRTFirstPolicy.class.getName(), true);
    }

    @Override
    public void initializePlugin(Configuration conf, MetaSchedulerCommService commService) {
        super.initializePlugin(conf, commService);
        maxCheck = conf.getLong(PosumConfiguration.REPRIORITIZE_INTERVAL,
                PosumConfiguration.REPRIORITIZE_INTERVAL_DEFAULT);
    }

    @Override
    protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
        CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
        capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
        return capacityConf;
    }

    @Override
    protected String resolveQueue(String queue, ApplicationId applicationId, String user, boolean isAppRecovering, ReservationId reservationID) {
        return YarnConfiguration.DEFAULT_QUEUE_NAME;
    }

    @Override
    protected String resolveMoveQueue(String queue, ApplicationId applicationId, String user) {
        return YarnConfiguration.DEFAULT_QUEUE_NAME;
    }

    protected JobProfile fetchJobProfile(String appId, String user) {
        DataBroker broker = commService.getDataBroker();
        if (broker == null)
            // DataMaster is not connected; do nothing
            return null;
        JobForAppCall getJobProfileForApp = JobForAppCall.newInstance(appId, user);
        JobProfile job = broker.executeDatabaseCall(getJobProfileForApp).getEntity();
        if (job == null) {
            logger.error("Could not retrieve job info for " + appId);
            return null;
        }
        return job;
    }

    @Override
    protected void updateAppPriority(SRTFAppAttempt app) {
        logger.debug("Updating app priority");
        try {
            String appId = app.getApplicationId().toString();
            JobProfile job = fetchJobProfile(appId, app.getUser());
            if (app.getJobId() == null) {
                if (job == null)
                    // something went wrong; do nothing
                    return;
                app.setJobId(job.getId());
                app.setSubmitTime(job.getSubmitTime());
            }
            if (job.getAvgMapDuration() != 0) {
                Long totalWork = job.getAvgMapDuration() * job.getTotalMapTasks();
                Long remainingWork = totalWork - job.getAvgMapDuration() * job.getCompletedMaps();
                if (job.getTotalReduceTasks() > 0) {
                    // there is reduce work to be done; get average task duration
                    long avgReduceDuration = job.getAvgReduceDuration();
                    long avgMapSize = job.getInputBytes() / job.getTotalMapTasks();
                    if (avgReduceDuration == 0 && avgMapSize != 0) {
                        // estimate avg reduce time
                        long totalReduceInputSize =
                                job.getMapOutputBytes() / job.getCompletedMaps() * job.getTotalMapTasks();
                        long reducerInputSize = totalReduceInputSize / job.getTotalReduceTasks();
                        avgReduceDuration = job.getAvgMapDuration() * reducerInputSize / avgMapSize;
                    }
                    remainingWork += avgReduceDuration * (job.getTotalReduceTasks() - job.getCompletedReduces());
                    totalWork += avgReduceDuration * job.getTotalReduceTasks();
                }
                app.setRemainingWork(remainingWork);
                app.setTotalWork(totalWork);
            }
        } catch (Exception e) {
            logger.debug("Could not update app priority for : " + app.getApplicationId(), e);
        }
    }

    @Override
    public Comparator<FiCaSchedulerApp> getApplicationComparator() {
        return new Comparator<FiCaSchedulerApp>() {
            @Override
            public int compare(FiCaSchedulerApp o1, FiCaSchedulerApp o2) {
                if (o1.getApplicationId().equals(o2.getApplicationId()))
                    return 0;
                SRTFAppAttempt srtf1 = (SRTFAppAttempt) o1, srtf2 = (SRTFAppAttempt) o2;
                return srtf1.getResourceDeficit() - srtf2.getResourceDeficit();
            }
        };
    }

    @Override
    protected boolean checkIfPrioritiesExpired() {
        long now = System.currentTimeMillis();
        if (now - lastCheck > maxCheck) {
            lastCheck = now;
            return true;
        }
        return false;
    }

    protected void updateApplicationPriorities(LeafQueue queue, String applicationSetName) {
        if (!applicationSetName.equals("activeApplications"))
            return;
        Set<FiCaSchedulerApp> apps = Utils.readField(queue, LeafQueue.class, applicationSetName);
        // calculate remaining times for each application and compute sum
        Double invertedSum = 0.0;
        for (Iterator<FiCaSchedulerApp> i = apps.iterator(); i.hasNext(); ) {
            SRTFAppAttempt app = (SRTFAppAttempt) i.next();
            updateAppPriority(app);
            if (app.getRemainingWork() != null)
                invertedSum += 1.0 / app.getRemainingTime(getMinimumResourceCapability());
        }
        for (Iterator<FiCaSchedulerApp> i = apps.iterator(); i.hasNext(); ) {
            SRTFAppAttempt app = (SRTFAppAttempt) i.next();
            // remove, update and add to resort
            i.remove();
            app.calculateDeficit(getMinimumResourceCapability(), getClusterResource(), invertedSum);
            apps.add(app);
        }
    }
}

