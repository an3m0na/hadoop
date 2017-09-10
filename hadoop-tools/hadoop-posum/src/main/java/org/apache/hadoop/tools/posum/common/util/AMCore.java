package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class AMCore {
  private final ResourceManager rm;
  private final String user;
  private final String queue;
  private ApplicationId appId;
  private ApplicationAttemptId appAttemptId;
  private final static int CONTAINER_MB = 1024;
  private final static int CONTAINER_VCORES = 1;
  private boolean amContainerRequested = false;

  private int RESPONSE_ID = 1;

  public AMCore(ResourceManager rm, String user, String queue) {
    this.rm = rm;
    this.user = user;
    this.queue = queue;
  }

  public ApplicationId create() throws YarnException {
    GetNewApplicationRequest newAppRequest =
      Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse newAppResponse =
      rm.getClientRMService().getNewApplication(newAppRequest);
    appId = newAppResponse.getApplicationId();
    return appId;
  }

  public void submit() throws IOException, InterruptedException, YarnException {
    if (appId == null)
      create();
    final SubmitApplicationRequest subAppRequest =
      Records.newRecord(SubmitApplicationRequest.class);
    ApplicationSubmissionContext appSubContext =
      Records.newRecord(ApplicationSubmissionContext.class);
    appSubContext.setApplicationId(appId);
    appSubContext.setMaxAppAttempts(1);
    appSubContext.setQueue(queue);
    appSubContext.setPriority(Priority.newInstance(0));
    ContainerLaunchContext conLauContext =
      Records.newRecord(ContainerLaunchContext.class);
    conLauContext.setApplicationACLs(
      new HashMap<ApplicationAccessType, String>());
    conLauContext.setCommands(new ArrayList<String>());
    conLauContext.setEnvironment(new HashMap<String, String>());
    conLauContext.setLocalResources(new HashMap<String, LocalResource>());
    conLauContext.setServiceData(new HashMap<String, ByteBuffer>());
    appSubContext.setAMContainerSpec(conLauContext);
    appSubContext.setUnmanagedAM(true);
    subAppRequest.setApplicationSubmissionContext(appSubContext);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws YarnException {
        rm.getClientRMService().submitApplication(subAppRequest);
        return null;
      }
    });
  }

  public void registerWithRM() throws YarnException, IOException, InterruptedException {
    waitForLaunch();

    final RegisterApplicationMasterRequest amRegisterRequest =
      Records.newRecord(RegisterApplicationMasterRequest.class);
    amRegisterRequest.setHost("localhost");
    amRegisterRequest.setRpcPort(1000);
    amRegisterRequest.setTrackingUrl("localhost:1000");

    createUGI().doAs(
      new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
        @Override
        public RegisterApplicationMasterResponse run() throws Exception {
          return rm.getApplicationMasterService()
            .registerApplicationMaster(amRegisterRequest);
        }
      });
  }

  private void waitForLaunch() throws InterruptedException {
    // waiting until application ACCEPTED
    while (getState() != RMAppState.ACCEPTED) {
      Thread.sleep(10);
    }

    // Waiting until application attempt reach LAUNCHED
    // "Unmanaged AM must register after AM attempt reaches LAUNCHED state"
    this.appAttemptId = rm.getRMContext().getRMApps().get(appId)
      .getCurrentAppAttempt().getAppAttemptId();
    RMAppAttempt rmAppAttempt = rm.getRMContext().getRMApps().get(appId)
      .getCurrentAppAttempt();
    while (rmAppAttempt.getAppAttemptState() != RMAppAttemptState.LAUNCHED) {
      Thread.sleep(10);
    }
  }

  public RMAppState getState(){
    return rm.getRMContext().getRMApps().get(appId).getState();
  }

  public AllocateResponse requestAMContainer() throws IOException, InterruptedException {
    List<ResourceRequest> ask = new ArrayList<>();
    ResourceRequest amRequest = createResourceRequest(
      BuilderUtils.newResource(CONTAINER_MB, CONTAINER_VCORES),
      ResourceRequest.ANY, 1, 1);
    ask.add(amRequest);
    amContainerRequested = true;
    return sendAllocateRequest(createAllocateRequest(ask));
  }

  public static ResourceRequest createResourceRequest(
    Resource resource, String host, int priority, int numContainers) {
    ResourceRequest request = Records.newRecord(ResourceRequest.class);
    request.setCapability(resource);
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    Priority prio = Records.newRecord(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    return request;
  }

  public AllocateRequest createAllocateRequest(List<ResourceRequest> ask,
                                               List<ContainerId> toRelease) {
    AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
    allocateRequest.setResponseId(RESPONSE_ID++);
    allocateRequest.setAskList(ask);
    allocateRequest.setReleaseList(toRelease);
    return allocateRequest;
  }

  public AllocateRequest createAllocateRequest(List<ResourceRequest> ask) {
    return createAllocateRequest(ask, new ArrayList<ContainerId>());
  }

  public AllocateResponse sendAllocateRequest(final AllocateRequest request) throws IOException, InterruptedException {
    return createUGI().doAs(
      new PrivilegedExceptionAction<AllocateResponse>() {
        @Override
        public AllocateResponse run() throws Exception {
          return rm.getApplicationMasterService().allocate(request);
        }
      });
  }

  public void unregister() throws IOException, InterruptedException {
    final FinishApplicationMasterRequest finishAMRequest = Records.newRecord(FinishApplicationMasterRequest.class);
    finishAMRequest.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);

    createUGI().doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        rm.getApplicationMasterService()
          .finishApplicationMaster(finishAMRequest);
        return null;
      }
    });
  }

  private UserGroupInformation createUGI() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps().get(appId)
      .getRMAppAttempt(appAttemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    return ugi;
  }

  public ApplicationId getAppId() {
    return appId;
  }

  public boolean isAmContainerRequested() {
    return amContainerRequested;
  }
}
