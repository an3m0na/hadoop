package org.apache.hadoop.tools.posum.common.util;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.OrchestratorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.apache.hadoop.tools.posum.database.client.Database;
import org.apache.hadoop.tools.posum.database.client.DatabaseImpl;
import org.apache.hadoop.tools.posum.database.store.LockBasedDataStore;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.util.Records;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

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
            throw new PosumException("Id parse exception for " + id, e);
        }
    }

    public static ApplicationId parseApplicationId(String id) {
        try {
            String[] parts = id.split("_");
            return ApplicationId.newInstance(Long.parseLong(parts[1]),
                    Integer.parseInt(parts[2]));
        } catch (Exception e) {
            throw new PosumException("Id parse exception for " + id, e);
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
            throw new PosumException("Id parse exception for " + id, e);
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
            throw new PosumException("Id parse exception for " + id, e);
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
            throw new PosumException("Id parse exception for " + id, e);
        }
    }

    public static <T extends Payload> SimpleResponse<T> handleError(String type, SimpleResponse<T> response) {
        if (!response.getSuccessful()) {
            throw new PosumException("Request type " + type + " returned with error: " +
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
            throw new PosumException("Could not construct request object for " + proto.getType(), e);
        }
    }

    public static <T extends Payload> SimpleResponse<T> wrapSimpleResponse(POSUMProtos.SimpleResponseProto proto) {
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
        OM("OrchestratorMaster",
                PosumConfiguration.PM_ADDRESS_DEFAULT + ":" + PosumConfiguration.PM_PORT_DEFAULT,
                OrchestratorMasterProtocol.class),
        DM("DataMaster",
                PosumConfiguration.DM_ADDRESS_DEFAULT + ":" + PosumConfiguration.DM_PORT_DEFAULT,
                DataMasterProtocol.class),
        SIMULATOR("SimulationMaster",
                PosumConfiguration.SIMULATOR_ADDRESS_DEFAULT + ":" + PosumConfiguration.SIMULATOR_PORT_DEFAULT,
                SimulatorMasterProtocol.class),
        SCHEDULER("PortfolioMetaScheduler",
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

    public static Field findField(Class startClass, String name)
            throws NoSuchFieldException {
        Class crtClass = startClass;
        while (crtClass != null) {
            Field[] fields = crtClass.getDeclaredFields();
            for (Field field : fields) {
                if (field.getName().equals(name)) {
                    return field;
                }
            }
            crtClass = crtClass.getSuperclass();
        }
        throw new NoSuchFieldException(startClass.getName() + "." + name);
    }

    public static Method findMethod(Class startClass, String name, Class<?>... paramTypes)
            throws NoSuchMethodException {
        Class crtClass = startClass;
        while (crtClass != null) {
            Method[] methods = crtClass.getDeclaredMethods();
            for (Method method : methods) {
                if (method.getName().equals(name)) {
                    if (Arrays.equals(method.getParameterTypes(), paramTypes)) {
                        return method;
                    }
                }
            }
            crtClass = crtClass.getSuperclass();
        }
        throw new NoSuchMethodException(startClass.getName() + "." + name +
                (paramTypes != null ? Arrays.asList(paramTypes).toString().replace('[', '(').replace(']', ')') : ""));
    }

    public static void writeField(Object object, Class startClass, String name, Object value) {
        try {
            Field field = Utils.findField(startClass, name);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new PosumException("Reflection error: ", e);
        }
    }

    public static <T> T readField(Object object, Class startClass, String name) {
        try {
            Field field = Utils.findField(startClass, name);
            field.setAccessible(true);
            return (T) field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new PosumException("Reflection error: ", e);
        }
    }


    public static <T> T invokeMethod(Object object, Class startClass, String name, Class<?>[] paramTypes, Object... args) {
        try {
            Method method = Utils.findMethod(startClass, name, paramTypes);
            method.setAccessible(true);
            return (T) method.invoke(object, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new PosumException("Reflection error: ", e);
        }
    }

    public static boolean safeEquals(Object o1, Object o2) {
        if (o1 == null)
            return o2 == null;
        return o1.equals(o2);
    }

    public static int safeHashCode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    public static boolean checkBeanPropertiesMatch(Object bean,
                                                   Map<String, Object> propertyValues)
            throws IntrospectionException, InvocationTargetException, IllegalAccessException {
        return checkBeanPropertiesMatch(
                bean,
                getBeanPropertyReaders(bean.getClass(), propertyValues.keySet()),
                propertyValues
        );
    }

    public static Map<String, Method> getBeanPropertyReaders(Class beanClass,
                                                             Set<String> propertyNames)
            throws IntrospectionException {
        Map<String, Method> ret = new HashMap<>(propertyNames.size());
        PropertyDescriptor[] descriptors =
                Introspector.getBeanInfo(beanClass, Object.class).getPropertyDescriptors();
        for (String name : propertyNames) {
            Method reader = findPropertyReader(descriptors, name);
            if (reader == null) {
                // explore name variations
                String alternatePropertyName = name.startsWith("_") ? name.substring(1) : "_" + name;
                reader = findPropertyReader(descriptors, alternatePropertyName);
                if (reader == null) {
                    if (name.contains("_"))
                        alternatePropertyName = WordUtils.capitalizeFully(name).replaceAll("_", "");
                    else
                        alternatePropertyName = name.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
                    reader = findPropertyReader(descriptors, alternatePropertyName);
                }
            }
            if (reader == null)
                throw new PosumException("Could not find property reader for " + name + " in " + beanClass);
            ret.put(name, reader);
        }
        return ret;
    }

    private static Method findPropertyReader(PropertyDescriptor[] propertyDescriptors, String propertyName) {
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (propertyName.equals(pd.getName())) {
                return pd.getReadMethod();
            }
        }
        return null;
    }

    public static boolean checkBeanPropertiesMatch(Object bean,
                                                   Map<String, Method> propertyReaders,
                                                   Map<String, Object> propertyValues)
            throws InvocationTargetException, IllegalAccessException {
        for (Map.Entry<String, Object> property : propertyValues.entrySet()) {
            if (!safeEquals(propertyReaders.get(property.getKey()).invoke(bean), property.getValue()))
                return false;
        }
        return true;
    }

    public static DataBroker exposeDataStoreAsBroker(final LockBasedDataStore dataStore) {
        return new DataBroker() {
            @Override
            public Database bindTo(DataEntityDB db) {
                return new DatabaseImpl(this, db);
            }

            @Override
            public <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call, DataEntityDB db) {
                return call.executeCall(dataStore, db);
            }

            @Override
            public Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections() {
                return dataStore.listExistingCollections();
            }

            @Override
            public void clear() {
                dataStore.clear();
            }
        };
    }
}
