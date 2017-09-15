package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.AMCore;
import org.apache.hadoop.tools.posum.common.util.DatabaseProvider;
import org.apache.hadoop.tools.posum.common.util.InjectableResourceScheduler;
import org.apache.hadoop.tools.posum.common.util.NMCore;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.SimplifiedResourceManager;
import org.apache.hadoop.tools.posum.data.mock.data.MockDataStoreImpl;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState.FINISHED;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public abstract class TestPolicy {
  private final Logger LOG = Logger.getLogger(TestPolicy.class);

  private final int SLOT_MB = 1024;
  private final int SLOT_CORES = 1;
  private final int SLOTS = 2;
  private final int MB_PER_NM = SLOTS * SLOT_MB;
  private final int CORES_PER_NM = SLOTS * SLOT_CORES;
  private final long MAX_WAIT = 1000L;

  private ResourceManager rm = null;
  private MockDataStoreImpl mockDataStore;
  protected Database db;
  protected YarnConfiguration conf;
  private List<NMCore> nodeManagers;
  private List<AMCore> allApps = new ArrayList<>();
  private List<JobProfile> allJobs = new ArrayList<>();
  private Class<?extends PluginPolicy> schedulerClass;
  private List<ApplicationId> alreadyAllocatedAMs = new ArrayList<>();

  protected TestPolicy(Class<? extends PluginPolicy> schedulerClass) {
    this.schedulerClass = schedulerClass;
  }

  @Before
  public void setUp() throws Exception {
    mockDataStore = new MockDataStoreImpl();
    db = Database.from(mockDataStore, DatabaseReference.getMain());
    conf = new YarnConfiguration(PosumConfiguration.newInstance());
    conf.setInt("yarn.nodemanager.resource.cpu-vcores", CORES_PER_NM);
    conf.setInt("yarn.nodemanager.resource.memory-mb", MB_PER_NM);
    conf.setInt("yarn.scheduler.minimum-allocation-vcores", SLOT_CORES);
    conf.setInt("yarn.scheduler.maximum-allocation-vcores", SLOT_CORES);
    conf.setInt("yarn.scheduler.minimum-allocation-mb", SLOT_MB);
    conf.setInt("yarn.scheduler.maximum-allocation-mb", SLOT_MB);
    rm = new SimplifiedResourceManager(new InjectableResourceScheduler(schedulerClass, new DatabaseProvider() {
      @Override
      public Database getDatabase() {
        return db;
      }
    }));
  }

  protected void startRM() {
    rm.init(conf);
    rm.start();
  }

  protected void registerNodes(int n) throws IOException, YarnException, InterruptedException {
    nodeManagers = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      NMCore nm = new NMCore(rm, "defaultRack", "192.168.1." + i, MB_PER_NM, CORES_PER_NM);
      nm.registerWithRM();
      nodeManagers.add(nm);
    }
    waitForNMs();
    assertThat(rm.getResourceScheduler().getClusterResource(),
      is(BuilderUtils.newResource(n * MB_PER_NM, n * CORES_PER_NM)));
  }

  private void waitForNMs() throws InterruptedException {
    long time = System.currentTimeMillis();
    while (rm.getResourceScheduler().getNumClusterNodes() < 2) {
//      if (System.currentTimeMillis() - time > MAX_WAIT)
//        throw new RuntimeException("Node managers were not registered in time");
      Thread.sleep(10);
    }
  }

  protected void submitApp(int id, long deadline) throws YarnException, IOException, InterruptedException {
    AMCore app = new AMCore(rm, "defaultUser", "default");
    app.create();
    allApps.add(app);
    addProfileForApp(app.getAppId());
    addJobForApp(app.getAppId(), deadline);

    LOG.debug("Submitting app" + id);
    app.submit();
    LOG.debug("Registering app" + id);
    app.registerWithRM();
    if(!app.requestAMContainer().getAllocatedContainers().isEmpty())
      alreadyAllocatedAMs.add(app.getAppId());
  }

  private void addProfileForApp(ApplicationId appId) {
    AppProfile app = Records.newRecord(AppProfile.class);
    app.setId(appId.toString());
    app.setStartTime(System.currentTimeMillis());
    db.execute(StoreCall.newInstance(APP, app));
  }

  private void addJobForApp(ApplicationId appId, long deadline) {
    JobProfile job = Records.newRecord(JobProfile.class);
    JobId jobId = Records.newRecord(JobId.class);
    jobId.setAppId(appId);
    jobId.setId(appId.getId());
    job.setId(jobId.toString());
    job.setAppId(appId.toString());
    job.setDeadline(deadline);
    db.execute(StoreCall.newInstance(JOB, job));
    allJobs.add(job);
  }

  protected boolean waitForAMContainer(AMCore app, int nmIndex) throws IOException, InterruptedException {
    if(alreadyAllocatedAMs.contains(app.getAppId()))
      return true;
    long time = System.currentTimeMillis();
    LOG.debug("Waiting for AM container for app" + app.getAppId().getId());
    for (AllocateResponse allocateResponse = sendHeartBeat(app);
         allocateResponse.getAllocatedContainers().isEmpty();
         allocateResponse = sendHeartBeat(app)) {
      if (System.currentTimeMillis() - time > MAX_WAIT) {
        LOG.debug("AM container was not allocated for app" + app.getAppId().getId());
        return false;
      }
      // send a heartbeat
      sendHeartBeat(nodeManagers.get(nmIndex));
      Thread.sleep(100);
      // try again
    }
    LOG.debug("AM is now running for app" + app.getAppId().getId());
    return true;
  }

  private AllocateResponse sendHeartBeat(AMCore am) throws IOException, InterruptedException {
    return am.sendAllocateRequest(am.createAllocateRequest(Collections.<ResourceRequest>emptyList()));
  }

  protected void sendHeartBeat(NMCore nm) {
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }

  protected boolean finishApp(int id) throws IOException, InterruptedException {
    AMCore app = getApp(id);
    LOG.debug("Shutting down app" + id);
    app.unregister();

    long time = System.currentTimeMillis();
    while (app.getState() != FINISHED) {
      if (System.currentTimeMillis() - time > MAX_WAIT) {
        LOG.debug("Shut down failed for app" + id);
        return false;
      }
      Thread.sleep(10);
    }
    LOG.debug("Shut down successful for app" + id);
    return true;
  }

  protected AMCore getApp(int id) {
    return allApps.get(id - 1);
  }

  protected JobProfile getJobForApp(int id) {
    return allJobs.get(id - 1);
  }

  protected int countAppsInQueue(String queue) {
    return rm.getResourceScheduler().getAppsInQueue(queue).size();
  }

  protected int countRMApps() {
    return rm.getRMContext().getRMApps().size();
  }

  protected void updatePriorities() {
    sendHeartBeat(nodeManagers.get(1));
  }

}
