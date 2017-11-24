package org.apache.hadoop.tools.posum.common.util;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CounterGroupInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.CounterInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.OrchestratorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;
import org.apache.hadoop.tools.posum.common.util.communication.StandardProtocol;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.PosumProtos;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.yarn.server.utils.BuilderUtils.newResourceRequest;

public class Utils {

  private static Log logger = LogFactory.getLog(Utils.class);
  public static final String ID_FIELD = "_id";
  public final static Priority DEFAULT_PRIORITY = Priority.newInstance(1);

  public static ApplicationId parseApplicationId(String id) {
    try {
      String[] parts = id.split("_");
      return ApplicationId.newInstance(Long.parseLong(parts[1]),
        Integer.parseInt(parts[2]));
    } catch (Exception e) {
      throw new PosumException("Id parse exception for " + id, e);
    }
  }

  private static JobId composeJobId(Long timestamp, Integer actualId) {
    return MRBuilderUtils.newJobId(ApplicationId.newInstance(timestamp, actualId), actualId);
  }

  public static JobId parseJobId(String id) {
    try {
      String[] parts = id.split("_");
      return composeJobId(Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
    } catch (Exception e) {
      throw new PosumException("Id parse exception for " + id, e);
    }
  }

  public static TaskId parseTaskId(String id) {
    try {
      String[] parts = id.split("_");
      return MRBuilderUtils.newTaskId(
        composeJobId(Long.parseLong(parts[1]), Integer.parseInt(parts[2])),
        Integer.parseInt(parts[4]),
        "m".equals(parts[3]) ? TaskType.MAP : TaskType.REDUCE
      );
    } catch (Exception e) {
      throw new PosumException("Id parse exception for " + id, e);
    }
  }

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
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
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
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
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

  public static void updateJobStatisticsFromCounters(JobProfile job, CountersProxy counters) {
    if (counters == null)
      return;
    for (CounterGroupInfoPayload group : counters.getCounterGroup()) {
      for (CounterInfoPayload counter : group.getCounter()) {
        switch (counter.getName()) {
          // make sure to record map materialized (compressed) bytes if compression is enabled
          // this is because reduce_shuffle_bytes are also in compressed form
          case "MAP_OUTPUT_BYTES":
            Long previous = orZero(job.getMapOutputBytes());
            if (previous == 0)
              job.setMapOutputBytes(counter.getTotalCounterValue());
            break;
          case "MAP_OUTPUT_MATERIALIZED_BYTES":
            Long value = counter.getTotalCounterValue();
            if (value > 0)
              job.setMapOutputBytes(value);
            break;
          case "REDUCE_SHUFFLE_BYTES":
            job.setReduceInputBytes(counter.getTotalCounterValue());
            break;
          case "BYTES_READ":
            job.setInputBytes(counter.getTotalCounterValue());
            break;
          case "BYTES_WRITTEN":
            job.setOutputBytes(counter.getTotalCounterValue());
            break;
        }
      }
    }
  }

  public static Long getDuration(JobProfile job) {
    if (job.getStartTime() == null || job.getFinishTime() == null || job.getFinishTime() == 0)
      return 0L;
    return job.getFinishTime() - job.getStartTime();
  }

  public static Long getDuration(TaskProfile task) {
    if (task.getStartTime() == null || task.getFinishTime() == null || task.getFinishTime() == 0)
      return 0L;
    return task.getFinishTime() - task.getStartTime();
  }

  public static void updateJobStatisticsFromTasks(JobProfile job, List<TaskProfile> tasks) {
    if (tasks == null)
      return;
    long mapDuration = 0, reduceDuration = 0, reduceTime = 0, shuffleTime = 0, mergeTime = 0;
    int mapNo = 0, reduceNo = 0, avgNo = 0;
    long mapInputSize = 0, mapOutputSize = 0, reduceInputSize = 0, reduceOutputSize = 0;

    for (TaskProfile task : tasks) {
      if (getDuration(task) <= 0)
        // skip unfinished tasks
        continue;
      if (TaskType.MAP.equals(task.getType())) {
        mapDuration += getDuration(task);
        mapNo++;
        mapInputSize += orZero(task.getInputBytes());
        mapOutputSize += orZero(task.getOutputBytes());
        if (task.getSplitLocations() != null && task.getHostName() != null) {
          if (task.getSplitLocations().contains(task.getHostName()))
            task.setLocal(true);
        }
      }
      if (TaskType.REDUCE.equals(task.getType())) {
        reduceDuration += getDuration(task);
        reduceTime += orZero(task.getReduceTime());
        shuffleTime += orZero(task.getShuffleTime());
        mergeTime += orZero(task.getMergeTime());
        reduceNo++;
        reduceInputSize += orZero(task.getInputBytes());
        reduceOutputSize += orZero(task.getOutputBytes());
      }
      avgNo++;
    }

    if (avgNo > 0) {
      if (mapNo > 0) {
        job.setAvgMapDuration(mapDuration / mapNo);
      }
      if (reduceNo > 0) {
        job.setAvgReduceDuration(reduceDuration / reduceNo);
        job.setAvgShuffleTime(shuffleTime / reduceNo);
        job.setAvgMergeTime(mergeTime / reduceNo);
        job.setAvgReduceTime(reduceTime / reduceNo);
      }
    }

    job.setInputBytes(mapInputSize);
    job.setMapOutputBytes(mapOutputSize);
    job.setReduceInputBytes(reduceInputSize);
    job.setOutputBytes(reduceOutputSize);
    job.setCompletedMaps(mapNo);
    job.setCompletedReduces(reduceNo);
  }

  public static void updateTaskStatisticsFromCounters(TaskProfile task, CountersProxy counters) {
    if (counters == null)
      return;
    for (CounterGroupInfoPayload group : counters.getCounterGroup()) {
      for (CounterInfoPayload counter : group.getCounter()) {
        switch (counter.getName()) {
          // make sure to record map materialized (compressed) bytes if compression is enabled
          // this is because reduce_shuffle_bytes are also in compressed form
          case "MAP_OUTPUT_BYTES":
            if (task.getType().equals(TaskType.MAP)) {
              Long previous = orZero(task.getOutputBytes());
              if (previous == 0)
                task.setOutputBytes(counter.getTotalCounterValue());
            }
            break;
          case "MAP_OUTPUT_MATERIALIZED_BYTES":
            if (task.getType().equals(TaskType.MAP)) {
              Long value = counter.getTotalCounterValue();
              if (value > 0)
                task.setOutputBytes(value);
            }
            break;
          case "REDUCE_SHUFFLE_BYTES":
            if (task.getType().equals(TaskType.REDUCE))
              task.setInputBytes(counter.getTotalCounterValue());
            break;
          case "BYTES_READ":
            if (task.getType().equals(TaskType.MAP)) {
              task.setInputBytes(counter.getTotalCounterValue());
            }
            break;
          case "BYTES_WRITTEN":
            if (task.getType().equals(TaskType.REDUCE))
              task.setOutputBytes(counter.getTotalCounterValue());
            break;
        }
      }
    }
  }

  public static long orZero(Long unsafeLong) {
    return unsafeLong == null ? 0 : unsafeLong;
  }

  public static float orZero(Float unsafeFloat) {
    return unsafeFloat == null ? 0 : unsafeFloat;
  }

  public static double orZero(Double unsafeDouble) {
    return unsafeDouble == null ? 0 : unsafeDouble;
  }

  public static int orZero(Integer unsafeInt) {
    return unsafeInt == null ? 0 : unsafeInt;
  }

  public static Boolean getBooleanField(JobProfile job, String fieldString, Boolean defaultValue) {
    String valueString = job.getFlexField(fieldString);
    return valueString == null ? defaultValue : Boolean.valueOf(valueString);
  }

  public static Double getDoubleField(JobProfile job, String fieldString, Double defaultValue) {
    String valueString = job.getFlexField(fieldString);
    return valueString == null ? defaultValue : Double.valueOf(valueString);
  }

  public static Long getLongField(JobProfile job, String fieldString, Long defaultValue) {
    String valueString = job.getFlexField(fieldString);
    return valueString == null ? defaultValue : Long.valueOf(valueString);
  }

  public static Integer getIntField(JobProfile job, String fieldString, Integer defaultValue) {
    String valueString = job.getFlexField(fieldString);
    return valueString == null ? defaultValue : Integer.valueOf(valueString);
  }

  public static void copyRunningAppInfo(DataStore dataStore, DatabaseReference source, DatabaseReference target) {
    Database simDb = Database.from(dataStore, target);
    simDb.clear();
    dataStore.copyCollections(source, target, Arrays.asList(APP, JOB, JOB_CONF, TASK, COUNTER));
  }

  public static ResourceRequest createResourceRequest(Resource resource,
                                                      String host,
                                                      int numContainers) {
    return newResourceRequest(DEFAULT_PRIORITY, host, resource, numContainers);
  }

  public static ResourceRequest createResourceRequest(Priority prioriy,
                                                      Resource resource,
                                                      String host,
                                                      int numContainers) {
    return newResourceRequest(prioriy, host, resource, numContainers);
  }

  public static DatabaseProvider newProvider(final Database db) {
    return new DatabaseProvider() {
      @Override
      public Database getDatabase() {
        return db;
      }
    };
  }
}
