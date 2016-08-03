package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.call.SaveJobFlexFieldsCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.core.scheduler.meta.MetaSchedulerCommService;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.extca.ExtCaSchedulerNode;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.extca.ExtensibleCapacityScheduler;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by ane on 5/31/16.
 */
public class EDLSPolicy<E extends EDLSPolicy> extends ExtensibleCapacityScheduler<EDLSAppAttempt, ExtCaSchedulerNode> {

    protected Log logger;

    protected final String DEADLINE_QUEUE = "deadline", BATCH_QUEUE = "default";
    protected final String DEADLINE_FLEX_KEY = EDLSPolicy.class.getSimpleName() + "::DEADLINE";
    protected final String TYPE_FLEX_KEY = EDLSPolicy.class.getSimpleName() + "::TYPE";
    private long lastCheck = 0;
    private long maxCheck;
    protected float deadlinePriority = PosumConfiguration.DC_PRIORITY_DEFAULT;

    public EDLSPolicy(Class<E> eClass) {
        super(EDLSAppAttempt.class, ExtCaSchedulerNode.class, eClass.getName(), true);
        logger = LogFactory.getLog(eClass);
    }

    @Override
    public void initializePlugin(Configuration conf, MetaSchedulerCommService commService) {
        super.initializePlugin(conf, commService);
        maxCheck = conf.getLong(PosumConfiguration.REPRIORITIZE_INTERVAL,
                PosumConfiguration.REPRIORITIZE_INTERVAL_DEFAULT);
        deadlinePriority = conf.getFloat(PosumConfiguration.DC_PRIORITY, PosumConfiguration.DC_PRIORITY_DEFAULT);
    }

    @Override
    protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
        CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
        capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
        return capacityConf;
    }

    @Override
    protected String resolveQueue(String queue,
                                  ApplicationId applicationId,
                                  String user,
                                  boolean isAppRecovering,
                                  ReservationId reservationID) {

        JobProfile job = fetchJobProfile(applicationId.toString(), user);
        if (job == null) {
            return BATCH_QUEUE;
        }
        if (EDLSAppAttempt.Type.DC.name().equals(job.getFlexField(TYPE_FLEX_KEY)))
            return DEADLINE_QUEUE;
        return BATCH_QUEUE;
    }

    @Override
    protected String resolveMoveQueue(String queue, ApplicationId applicationId, String user) {
        return resolveQueue(queue, applicationId, user, false, null);
    }

    protected JobProfile fetchJobProfile(String appId, String user) {
        DataBroker broker = commService.getDataBroker();
        if (broker == null)
            // DataMaster is not connected; do nothing
            return null;
        JobProfile job = broker.executeDatabaseCall(JobForAppCall.newInstance(appId, user)).getEntity();
        if (job == null) {
            logger.error("Could not retrieve job info for " + appId);
            return null;
        }
        Map<String, String> flexFields = job.getFlexFields();
        String typeString = flexFields.get(TYPE_FLEX_KEY);
        if (typeString == null) {
            // first initialization
            FindByIdCall getJobConf = FindByIdCall.newInstance(DataEntityCollection.JOB_CONF, job.getId());
            JobConfProxy confProxy = broker.executeDatabaseCall(getJobConf).getEntity();
            String deadlineString = confProxy.getEntry(PosumConfiguration.APP_DEADLINE);
            Map<String, String> newFields = new HashMap<>(2);
            if (deadlineString != null && deadlineString.length() > 0) {
                newFields.put(DEADLINE_FLEX_KEY, deadlineString);
                newFields.put(TYPE_FLEX_KEY, EDLSAppAttempt.Type.DC.name());
            } else {
                newFields.put(TYPE_FLEX_KEY, EDLSAppAttempt.Type.BC.name());
            }
            SaveJobFlexFieldsCall saveFlexFields = SaveJobFlexFieldsCall.newInstance(job.getId(), newFields, false);
            broker.executeDatabaseCall(saveFlexFields);
        }
        return job;
    }

    @Override
    protected void updateAppPriority(EDLSAppAttempt app) {
        logger.debug("Updating app priority");
        try {
            if (EDLSAppAttempt.Type.DC.equals(app.getType()))
                // application should already be initialized; do nothing
                return;
            if (commService.getDataBroker() == null)
                // DataMaster is not connected; do nothing
                return;
            String appId = app.getApplicationId().toString();
            JobProfile job = fetchJobProfile(appId, app.getUser());
            if (app.getType() == null) {
                if (job == null)
                    // something went wrong; do nothing
                    return;
                app.setJobId(job.getId());
                app.setSubmitTime(job.getSubmitTime());
                EDLSAppAttempt.Type type =
                        EDLSAppAttempt.Type.valueOf(job.getFlexField(TYPE_FLEX_KEY));
                app.setType(type);
                if (EDLSAppAttempt.Type.DC.equals(type)) {
                    app.setDeadline(Long.valueOf(job.getFlexField(DEADLINE_FLEX_KEY)));
                }
            }
            app.setExecutionTime(job.getAvgMapDuration() * job.getCompletedMaps() +
                    job.getAvgReduceDuration() * job.getCompletedReduces());
            // this is a batch job; update its slowdown
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
                EDLSAppAttempt edls1 = (EDLSAppAttempt) o1, edls2 = (EDLSAppAttempt) o2;
                if (EDLSAppAttempt.Type.DC.equals(edls1.getType()) && EDLSAppAttempt.Type.DC.equals(edls2.getType()))
                    // both dc jobs, earliest deadline first
                    return new Long(edls1.getRemaining() - edls2.getRemaining()).intValue();
                if (EDLSAppAttempt.Type.DC.equals(edls1.getType()))
                    // only elds1 is dc, it has priority
                    return -1;
                if (EDLSAppAttempt.Type.DC.equals(edls2.getType()))
                    // only elds2 is dc, it has priority
                    return 1;
                // both are bc jobs
                if (edls1.getSlowdown().equals(edls2.getSlowdown()))
                    // apply FIFO
                    return edls1.getApplicationId().compareTo(edls2.getApplicationId());
                // largest slowdown first
                return new Double(Math.signum(edls2.getSlowdown() - edls1.getSlowdown())).intValue();
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
}
