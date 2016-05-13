package org.apache.hadoop.tools.posum.common.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.POSUMMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorProtocol;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.util.Records;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by ane on 3/3/16.
 */
public class Utils {

    private static Log logger = LogFactory.getLog(Utils.class);

    public static TaskType getTaskTypeFromId(String id) {
        try {
            String[] parts = id.split("_");
            return "m".equals(parts[parts.length - 2]) ? TaskType.MAP : TaskType.REDUCE;
        } catch (Exception e) {
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static ApplicationId parseApplicationId(String id) {
        try {
            String[] parts = id.split("_");
            return ApplicationId.newInstance(Long.parseLong(parts[1]),
                    Integer.parseInt(parts[2]));
        } catch (Exception e) {
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static JobId parseJobId(String appId, String id) {
        try {
            String[] parts = id.split("_");
            JobId jobId = Records.newRecord(JobId.class);
            jobId.setAppId(parseApplicationId(appId));
            jobId.setId(Integer.parseInt(parts[parts.length - 1]));
            return jobId;
        } catch (Exception e) {
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static TaskId parseTaskId(String appId, String id) {
        try {
            String[] parts = id.split("_");
            TaskId taskId = Records.newRecord(TaskId.class);
            taskId.setJobId(parseJobId(appId, parts[0] + "_" + parts[1] + "_" + parts[2]));
            taskId.setTaskType("m".equals(parts[3]) ? TaskType.MAP : TaskType.REDUCE);
            taskId.setId(Integer.parseInt(parts[4]));
            return taskId;
        } catch (Exception e) {
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static TaskId parseTaskId(String id) {
        try {
            String[] parts = id.split("_");
            TaskId taskId = Records.newRecord(TaskId.class);
            taskId.setJobId(parseJobId("application_" + parts[1] + "0000", parts[0] + "_" + parts[1] + "_" + parts[2]));
            taskId.setTaskType("m".equals(parts[3]) ? TaskType.MAP : TaskType.REDUCE);
            taskId.setId(Integer.parseInt(parts[4]));
            return taskId;
        } catch (Exception e) {
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static <T> SimpleResponse<T> handleError(String type, SimpleResponse<T> response) {
        if (!response.getSuccessful()) {
            throw new POSUMException("Request type " + type + " returned with error: " +
                    "\n" + response.getText() + "\n" + response.getException());
        }
        return response;
    }

    public static <T> SimpleRequest<T> wrapSimpleRequest(POSUMProtos.SimpleRequestProto proto) {
        try {
            Class<? extends SimpleRequestPBImpl> implClass =
                    SimpleRequest.Type.fromProto(proto.getType()).getImplClass();
            return implClass.getConstructor(POSUMProtos.SimpleRequestProto.class).newInstance(proto);
        } catch (Exception e) {
            throw new POSUMException("Could not construct request object for " + proto.getType(), e);
        }
    }

    public static SimpleResponse wrapSimpleResponse(POSUMProtos.SimpleResponseProto proto) {
        try {
            Class<? extends SimpleResponsePBImpl> implClass =
                    SimpleResponse.Type.fromProto(proto.getType()).getImplClass();
            return implClass.getConstructor(POSUMProtos.SimpleResponseProto.class).newInstance(proto);
        } catch (Exception e) {
            throw new POSUMException("Could not construct response object", e);
        }
    }

    public static String getErrorTrace(Throwable e) {
        StringWriter traceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(traceWriter));
        return traceWriter.toString();
    }

    public enum POSUMProcess {
        PM("POSUMMaster", POSUMMasterProtocol.class),
        DM("DataMaster", DataMasterProtocol.class),
        SIMULATOR("SimulationMaster", SimulatorProtocol.class),
        SCHEDULER("PortfolioMetaScheduler", MetaSchedulerProtocol.class);

        private final String longName;
        private final Class<? extends StandardProtocol> accessorProtocol;

        POSUMProcess(String longName, Class<? extends StandardProtocol> accessorProtocol) {
            this.longName = longName;
            this.accessorProtocol = accessorProtocol;
        }
    }
}
