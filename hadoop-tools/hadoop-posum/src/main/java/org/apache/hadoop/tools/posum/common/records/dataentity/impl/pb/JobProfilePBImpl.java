package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.StringListPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.StringStringMapPayloadPBImpl;
import org.apache.hadoop.yarn.proto.PosumProtos.JobProfileProto;
import org.apache.hadoop.yarn.proto.PosumProtos.JobProfileProtoOrBuilder;
import org.apache.hadoop.yarn.proto.PosumProtos.StringListPayloadProto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JobProfilePBImpl extends GeneralDataEntityPBImpl<JobProfile, JobProfileProto, JobProfileProto.Builder>
  implements JobProfile {

  public JobProfilePBImpl() {
  }

  public JobProfilePBImpl(JobProfileProto proto) {
    super(proto);
  }

  @Override
  void initBuilder() {
    builder = viaProto ? JobProfileProto.newBuilder(proto) : JobProfileProto.newBuilder();
  }

  private Map<String, String> flexMap;
  private List<List<String>> splitLocations;
  private List<Long> splitSizes;

  @Override
  void buildProto() {
    maybeInitBuilder();
    if (flexMap != null) {
      builder.clearFlexFields();
      StringStringMapPayloadPBImpl flexFields = new StringStringMapPayloadPBImpl();
      flexFields.setEntries(flexMap);
      builder.setFlexFields(flexFields.getProto());
    }
    if (splitLocations != null) {
      builder.clearSplitLocations();
      Iterable<StringListPayloadProto> iterable =
        new Iterable<StringListPayloadProto>() {

          @Override
          public Iterator<StringListPayloadProto> iterator() {
            return new Iterator<StringListPayloadProto>() {

              Iterator<List<String>> entryIterator = splitLocations.iterator();

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public StringListPayloadProto next() {
                List<String> entry = entryIterator.next();
                StringListPayloadPBImpl impl = new StringListPayloadPBImpl();
                impl.setEntries(entry);
                return impl.getProto();
              }

              @Override
              public boolean hasNext() {
                return entryIterator.hasNext();
              }
            };
          }
        };
      builder.addAllSplitLocations(iterable);
    }
    if (splitSizes != null) {
      builder.clearSplitSizes();
      builder.addAllSplitSizes(splitSizes);
    }
    proto = builder.build();
  }

  @Override
  public JobProfile parseToEntity(ByteString data) throws InvalidProtocolBufferException {
    this.proto = JobProfileProto.parseFrom(data);
    viaProto = true;
    return this;
  }

  @Override
  public String getId() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasId())
      return null;
    return p.getId();
  }

  @Override
  public void setId(String id) {
    maybeInitBuilder();
    if (id == null) {
      builder.clearId();
      return;
    }
    builder.setId(id);
  }

  @Override
  public Long getLastUpdated() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLastUpdated())
      return null;
    return p.getLastUpdated();
  }

  @Override
  public void setLastUpdated(Long timestamp) {
    maybeInitBuilder();
    if (timestamp == null) {
      builder.clearLastUpdated();
      return;
    }
    builder.setLastUpdated(timestamp);
  }

  @Override
  public JobProfile copy() {
    return new JobProfilePBImpl(getProto());
  }

  @Override
  public Long getStartTime() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStartTime())
      return null;
    return p.getStartTime();
  }

  @Override
  public void setStartTime(Long startTime) {
    maybeInitBuilder();
    if (startTime == null) {
      builder.clearStartTime();
      return;
    }
    builder.setStartTime(startTime);
  }

  @Override
  public String getUser() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser())
      return null;
    return p.getUser();
  }

  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    if (user == null) {
      builder.clearUser();
      return;
    }
    builder.setUser(user);
  }

  @Override
  public String getName() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName())
      return null;
    return p.getName();
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName(name);
  }

  @Override
  public Long getFinishTime() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFinishTime())
      return null;
    return p.getFinishTime();
  }

  @Override
  public void setFinishTime(Long finishTime) {
    maybeInitBuilder();
    if (finishTime == null) {
      builder.clearFinishTime();
      return;
    }
    builder.setFinishTime(finishTime);
  }

  @Override
  public void setTotalMapTasks(Integer totalMapTasks) {
    maybeInitBuilder();
    if (totalMapTasks == null) {
      builder.clearTotalMapTasks();
      return;
    }
    builder.setTotalMapTasks(totalMapTasks);
  }

  @Override
  public void setTotalReduceTasks(Integer totalReduceTasks) {
    maybeInitBuilder();
    if (totalReduceTasks == null) {
      builder.clearTotalReduceTasks();
      return;
    }
    builder.setTotalReduceTasks(totalReduceTasks);
  }

  @Override
  public Long getTotalSplitSize() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTotalSplitSize())
      return null;
    return p.getTotalSplitSize();
  }

  @Override
  public void setTotalSplitSize(Long inputBytes) {
    maybeInitBuilder();
    if (inputBytes == null) {
      builder.clearTotalSplitSize();
      return;
    }
    builder.setTotalSplitSize(inputBytes);
  }

  @Override
  public Long getInputBytes() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasInputBytes())
      return null;
    return p.getInputBytes();
  }

  @Override
  public void setInputBytes(Long inputBytes) {
    maybeInitBuilder();
    if (inputBytes == null) {
      builder.clearInputBytes();
      return;
    }
    builder.setInputBytes(inputBytes);
  }

  @Override
  public Long getMapOutputBytes() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMapOutputBytes())
      return null;
    return p.getMapOutputBytes();
  }

  @Override
  public void setMapOutputBytes(Long bytes) {
    maybeInitBuilder();
    if (bytes == null) {
      builder.clearMapOutputBytes();
      return;
    }
    builder.setMapOutputBytes(bytes);
  }

  @Override
  public Long getReduceInputBytes() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasReduceInputBytes())
      return null;
    return p.getReduceInputBytes();
  }

  @Override
  public void setReduceInputBytes(Long bytes) {
    maybeInitBuilder();
    if (bytes == null) {
      builder.clearReduceInputBytes();
      return;
    }
    builder.setReduceInputBytes(bytes);
  }

  @Override
  public Long getOutputBytes() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasOutputBytes())
      return null;
    return p.getOutputBytes();
  }

  @Override
  public void setOutputBytes(Long outputBytes) {
    maybeInitBuilder();
    if (outputBytes == null) {
      builder.clearOutputBytes();
      return;
    }
    builder.setOutputBytes(outputBytes);
  }

  @Override
  public Long getSubmitTime() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSubmitTime())
      return null;
    return p.getSubmitTime();
  }

  @Override
  public void setSubmitTime(Long submitTime) {
    maybeInitBuilder();
    if (submitTime == null) {
      builder.clearSubmitTime();
      return;
    }
    builder.setSubmitTime(submitTime);
  }

  @Override
  public Integer getTotalMapTasks() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTotalMapTasks())
      return 0;
    return p.getTotalMapTasks();
  }

  @Override
  public Integer getTotalReduceTasks() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTotalReduceTasks())
      return 0;
    return p.getTotalReduceTasks();
  }

  @Override
  public String getAppId() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAppId())
      return null;
    return p.getAppId();
  }

  @Override
  public void setAppId(String appId) {
    maybeInitBuilder();
    if (appId == null) {
      builder.clearAppId();
      return;
    }
    builder.setAppId(appId);
  }

  @Override
  public JobState getState() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState())
      return null;
    return JobState.valueOf(p.getState().name().substring("STATE_".length()));
  }

  @Override
  public void setState(JobState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(JobProfileProto.JobStateProto.valueOf("STATE_" + state.name()));
  }

  @Override
  public Float getMapProgress() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMapProgress())
      return null;
    return p.getMapProgress();
  }

  @Override
  public void setMapProgress(Float mapProgress) {
    maybeInitBuilder();
    if (mapProgress == null) {
      builder.clearMapProgress();
      return;
    }
    builder.setMapProgress(mapProgress);
  }

  @Override
  public Float getReduceProgress() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasReduceProgress())
      return null;
    return p.getReduceProgress();
  }

  @Override
  public void setReduceProgress(Float reduceProgress) {
    maybeInitBuilder();
    if (reduceProgress == null) {
      builder.clearReduceProgress();
      return;
    }
    builder.setReduceProgress(reduceProgress);
  }

  @Override
  public Integer getCompletedMaps() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasCompletedMaps())
      return 0;
    return p.getCompletedMaps();
  }

  @Override
  public void setCompletedMaps(Integer completedMaps) {
    maybeInitBuilder();
    if (completedMaps == null) {
      builder.clearCompletedMaps();
      return;
    }
    builder.setCompletedMaps(completedMaps);
  }

  @Override
  public Integer getCompletedReduces() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasCompletedReduces())
      return 0;
    return p.getCompletedReduces();
  }

  @Override
  public void setCompletedReduces(Integer completedReduces) {
    maybeInitBuilder();
    if (completedReduces == null) {
      builder.clearCompletedReduces();
      return;
    }
    builder.setCompletedReduces(completedReduces);
  }

  @Override
  public Boolean isUberized() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUberized())
      return null;
    return p.getUberized();
  }

  @Override
  public void setUberized(Boolean uberized) {
    maybeInitBuilder();
    if (uberized == null) {
      builder.clearUberized();
      return;
    }
    builder.setUberized(uberized);
  }

  @Override
  public Long getAvgMapDuration() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAvgMapDuration())
      return null;
    return p.getAvgMapDuration();
  }

  @Override
  public void setAvgMapDuration(Long avgMapDuration) {
    maybeInitBuilder();
    if (avgMapDuration == null) {
      builder.clearAvgMapDuration();
      return;
    }
    builder.setAvgMapDuration(avgMapDuration);
  }

  @Override
  public Long getAvgReduceDuration() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAvgReduceDuration())
      return null;
    return p.getAvgReduceDuration();
  }

  @Override
  public void setAvgReduceDuration(Long duration) {
    maybeInitBuilder();
    if (duration == null) {
      builder.clearAvgReduceDuration();
      return;
    }
    builder.setAvgReduceDuration(duration);
  }

  @Override
  public void setAvgShuffleTime(Long avgShuffleDuration) {
    maybeInitBuilder();
    if (avgShuffleDuration == null) {
      builder.clearAvgShuffleTime();
      return;
    }
    builder.setAvgShuffleTime(avgShuffleDuration);
  }

  @Override
  public Long getAvgShuffleTime() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAvgShuffleTime())
      return null;
    return p.getAvgShuffleTime();
  }

  @Override
  public void setAvgMergeTime(Long time) {
    maybeInitBuilder();
    if (time == null) {
      builder.clearAvgMergeTime();
      return;
    }
    builder.setAvgMergeTime(time);
  }

  @Override
  public Long getAvgMergeTime() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAvgMergeTime())
      return null;
    return p.getAvgMergeTime();
  }

  @Override
  public void setAvgReduceTime(Long time) {
    maybeInitBuilder();
    if (time == null) {
      builder.clearAvgReduceTime();
      return;
    }
    builder.setAvgReduceTime(time);
  }

  @Override
  public Long getAvgReduceTime() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAvgReduceTime())
      return null;
    return p.getAvgReduceTime();
  }

  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(queue);

  }

  @Override
  public String getQueue() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    return p.getQueue();
  }

  @Override
  public void addAllFlexFields(Map<String, String> other) {
    maybeInitBuilder();
    if (other != null) {
      maybeInitFlexFields();
      if (flexMap == null)
        flexMap = new HashMap<>(other.size());
      flexMap.putAll(other);
    }
  }

  @Override
  public String getFlexField(String name) {
    maybeInitFlexFields();
    return flexMap == null ? null : flexMap.get(name);
  }

  @Override
  public Map<String, String> getFlexFields() {
    maybeInitFlexFields();
    return flexMap;
  }

  public void setFlexFields(Map<String, String> flexFields) {
    if (flexFields == null) {
      builder.clearFlexFields();
      flexMap = null;
      return;
    }
    flexMap = new HashMap<>(flexFields.size());
    flexMap.putAll(flexFields);
  }

  private void maybeInitFlexFields() {
    if (flexMap == null) {
      JobProfileProtoOrBuilder p = viaProto ? proto : builder;
      if (p.hasFlexFields()) {
        flexMap = new HashMap<>();
        flexMap.putAll(new StringStringMapPayloadPBImpl(p.getFlexFields()).getEntries());
      }
    }
  }

  @Override
  public String getReducerClass() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasReducerClass())
      return null;
    return p.getReducerClass();
  }

  @Override
  public void setMapperClass(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearMapperClass();
      return;
    }
    builder.setMapperClass(name);
  }

  @Override
  public String getMapperClass() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMapperClass())
      return null;
    return p.getMapperClass();
  }

  @Override
  public void setReducerClass(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearReducerClass();
      return;
    }
    builder.setReducerClass(name);

  }

  @Override
  public List<List<String>> getSplitLocations() {
    if (splitLocations == null) {
      JobProfileProtoOrBuilder p = viaProto ? proto : builder;
      List<StringListPayloadProto> protoList = p.getSplitLocationsList();
      splitLocations = new ArrayList<>(protoList.size());
      for (StringListPayloadProto stringListPayloadProto : protoList) {
        splitLocations.add(new StringListPayloadPBImpl(stringListPayloadProto).getEntries());
      }
    }
    return splitLocations;
  }

  @Override
  public void setSplitSizes(List<Long> splitSizes) {
    maybeInitBuilder();
    if (splitSizes == null) {
      builder.clearSplitSizes();
    }
    this.splitSizes = splitSizes;
  }

  @Override
  public List<Long> getSplitSizes() {
    if (splitSizes == null) {
      JobProfileProtoOrBuilder p = viaProto ? proto : builder;
      splitSizes = new ArrayList<>(p.getSplitSizesList());
    }
    return splitSizes;
  }

  @Override
  public void setSplitLocations(List<List<String>> splitLocations) {
    maybeInitBuilder();
    if (splitLocations == null) {
      builder.clearSplitLocations();
    }
    this.splitLocations = splitLocations;
  }

  @Override
  public Long getDeadline() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDeadline())
      return null;
    return p.getDeadline();
  }

  @Override
  public void setDeadline(Long deadline) {
    maybeInitBuilder();
    if (deadline == null) {
      builder.clearDeadline();
      return;
    }
    builder.setDeadline(deadline);
  }

  @Override
  public String getHostName() {
    JobProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHostName())
      return null;
    return p.getHostName();
  }

  @Override
  public void setHostName(String hostName) {
    maybeInitBuilder();
    if (hostName == null) {
      builder.clearHostName();
      return;
    }
    builder.setHostName(hostName);
  }
}
