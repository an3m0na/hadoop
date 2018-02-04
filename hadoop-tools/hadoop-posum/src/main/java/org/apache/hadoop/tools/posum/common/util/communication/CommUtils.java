package org.apache.hadoop.tools.posum.common.util.communication;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.OrchestratorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.PosumProtos;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

public class CommUtils {
  public static void checkPing(StandardProtocol handler) {
    sendSimpleRequest("ping", SimpleRequest.newInstance(SimpleRequest.Type.PING, "Hello world!"), handler);
  }

  public static <T extends Payload> T sendSimpleRequest(SimpleRequest.Type type, StandardProtocol handler) {
    return sendSimpleRequest(type.name(), SimpleRequest.newInstance(type), handler);
  }

  public static <T extends Payload> T sendSimpleRequest(String label, SimpleRequest request, StandardProtocol handler) {
    try {
      return (T) handleError(label, handler.handleSimpleRequest(request)).getPayload();
    } catch (IOException | YarnException e) {
      throw new PosumException("Error during RPC call", e);
    }
  }

  public static <T extends Payload> SimpleResponse<T> handleError(String label, SimpleResponse<T> response) {
    if (!response.getSuccessful()) {
      throw new PosumException("Request " + label + " returned with error: " +
        "\n" + response.getText() + "\n" + response.getException());
    }
    return response;
  }

  public static <T extends Payload> SimpleResponse<T> wrapSimpleResponse(PosumProtos.SimpleResponseProto proto) {
    try {
      return new SimpleResponsePBImpl(proto);
    } catch (Exception e) {
      throw new PosumException("Could not construct response object", e);
    }
  }

  public static String getErrorTrace(Throwable e) {
    StringWriter traceWriter = new StringWriter();
    e.printStackTrace(new PrintWriter(traceWriter));
    return traceWriter.toString();
  }

  public enum PosumProcess {
    OM("OrchestrationMaster",
      PosumConfiguration.PM_ADDRESS_DEFAULT + ":" + PosumConfiguration.PM_PORT_DEFAULT,
      OrchestratorMasterProtocol.class),
    DM("DataMaster",
      PosumConfiguration.DM_ADDRESS_DEFAULT + ":" + PosumConfiguration.DM_PORT_DEFAULT,
      DataMasterProtocol.class),
    SM("SimulationMaster",
      PosumConfiguration.SIMULATOR_ADDRESS_DEFAULT + ":" + PosumConfiguration.SIMULATOR_PORT_DEFAULT,
      SimulatorMasterProtocol.class),
    PS("PortfolioMetaScheduler",
      PosumConfiguration.SCHEDULER_ADDRESS_DEFAULT + ":" + PosumConfiguration.SCHEDULER_PORT_DEFAULT,
      MetaSchedulerProtocol.class);

    private final String longName;
    private String address;
    private final Class<? extends StandardProtocol> accessorProtocol;

    PosumProcess(String longName, String address, Class<? extends StandardProtocol> accessorProtocol) {
      this.longName = longName;
      this.address = address;
      this.accessorProtocol = accessorProtocol;
    }

    public String getLongName() {
      return longName;
    }

    public Class<? extends StandardProtocol> getAccessorProtocol() {
      return accessorProtocol;
    }

    public String getAddress() {
      return address;
    }

    public void setAddress(String address) {
      this.address = address;
    }
  }
}
