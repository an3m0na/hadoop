package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.Locality;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState.FINISHED;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public abstract class TestPolicy<T extends PluginPolicy> {
  private final Logger LOG = Logger.getLogger(TestPolicy.class);

  private final int SLOT_MB = 1024;
  private final int SLOT_CORES = 1;
  private final int SLOTS = 2;
  private final int MB_PER_NM = SLOTS * SLOT_MB;
  private final int CORES_PER_NM = SLOTS * SLOT_CORES;
  private final long MAX_WAIT = 1000L;

  private ResourceManager rm = null;
  protected Database db;
  protected YarnConfiguration conf;
  private List<NMCore> nodeManagers;
  private List<AMCore> allApps = new ArrayList<>();
  private List<JobProfile> allJobs = new ArrayList<>();
  private Class<? extends PluginPolicy> schedulerClass;
  private Map<ApplicationId, List<Container>> allocatedContainers = new HashMap<>();

  protected TestPolicy(Class<T> schedulerClass) {
    this.schedulerClass = schedulerClass;
  }

  @Before
  public void setUp() throws Exception {
    db = Database.from(new MockDataStoreImpl(), DatabaseReference.getMain());
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
      NMCore nm = new NMCore(rm, "/rack-unknown", "192.168.1." + i, MB_PER_NM, CORES_PER_NM);
      nm.registerWithRM();
      nodeManagers.add(nm);
    }
    waitForNMs(n);

    assertThat(rm.getResourceScheduler().getClusterResource(),
      is(BuilderUtils.newResource(n * MB_PER_NM, n * CORES_PER_NM)));
  }

  private void waitForNMs(int n) throws InterruptedException {
    long time = System.currentTimeMillis();
    while (rm.getResourceScheduler().getNumClusterNodes() < n) {
//      if (System.currentTimeMillis() - time > MAX_WAIT)
//        throw new RuntimeException("Node managers were not registered in time");
      Thread.sleep(10);
    }
  }

  protected void submitApp(int id, long deadline) throws YarnException, IOException, InterruptedException {
    submitApp(id, deadline, null, null);
  }

  protected void submitApp(int id) throws YarnException, IOException, InterruptedException {
    submitApp(id, 0, null, null);
  }

  protected void submitAppToNode(int id, int nmIndex, Locality locality) throws YarnException, IOException, InterruptedException {
    NMCore nm = nodeManagers.get(nmIndex);
    submitApp(id, 0L, nm, locality);
  }

  private void submitApp(int id, long deadline, NMCore nm, Locality locality) throws YarnException, IOException, InterruptedException {
    AMCore app = new AMCore(rm, "defaultUser", "default");
    app.create();
    allApps.add(app);
    addProfileForApp(app.getAppId());
    addJobForApp(app.getAppId(), deadline);

    LOG.debug("Submitting app" + id);
    app.submit();
    LOG.debug("Registering app" + id);
    app.registerWithRM();
    allocatedContainers.put(app.getAppId(), app.requestAMContainer(nm, locality).getAllocatedContainers());
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

    long time = System.currentTimeMillis();
    LOG.debug("Waiting for AM container for app" + app.getAppId().getId());
    while (getOrRequestContainers(app).isEmpty()) {
      if (System.currentTimeMillis() - time > MAX_WAIT) {
        LOG.debug("AM container was not allocated for app" + app.getAppId().getId());
        return false;
      }
      // send a heartbeat
      sendNodeUpdate(nmIndex);
      Thread.sleep(100);
      // try again
    }
    LOG.debug("AM is now running for app" + app.getAppId().getId());
    return true;
  }

  protected List<Container> getOrRequestContainers(AMCore am) throws IOException, InterruptedException {
    List<Container> allocated = allocatedContainers.get(am.getAppId());
    if (allocated.isEmpty())
      return am.sendAllocateRequest().getAllocatedContainers();
    return allocated;
  }

  protected int getAMNodeIndex(int appId) throws IOException, InterruptedException {
    List<Container> containers = getOrRequestContainers(getApp(appId));
    if (containers.isEmpty())
      return -1;
    for (int i = 0; i < nodeManagers.size(); i++) {
      if (nodeManagers.get(i).getNodeId().equals(containers.get(0).getNodeId()))
        return i;
    }
    return -1;
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

  protected void sendNodeUpdate(int index) {
    RMNode node = rm.getRMContext().getRMNodes().get(nodeManagers.get(index).getNodeId());
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }

  protected void defaultSmokeTest() throws Exception {
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 1f); // all apps can run

    startRM();
    registerNodes(2);

    submitApp(1);
    assertTrue(waitForAMContainer(getApp(1), 0));
    submitApp(2);
    assertTrue(waitForAMContainer(getApp(2), 0));

    submitApp(3);
    assertFalse(waitForAMContainer(getApp(3), 0));
    assertTrue(waitForAMContainer(getApp(3), 1));

    submitApp(4);
    assertFalse(waitForAMContainer(getApp(4), 0));
    assertTrue(waitForAMContainer(getApp(4), 1));

    submitApp(5);
    assertFalse(waitForAMContainer(getApp(5), 0));
    assertFalse(waitForAMContainer(getApp(5), 1));

    assertThat(countAppsInQueue("default"), is(5));
    assertThat(countRMApps(), is(5));

    finishApp(4);
    assertFalse(waitForAMContainer(getApp(5), 0));
    assertTrue(waitForAMContainer(getApp(5), 1));
  }
}
