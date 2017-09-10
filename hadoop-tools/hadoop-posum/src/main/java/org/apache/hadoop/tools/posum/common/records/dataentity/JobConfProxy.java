package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public interface JobConfProxy extends GeneralDataEntity<JobConfProxy> {

  @JsonIgnore
  String getEntry(String name);

  @JsonIgnore
  Configuration getConf();

  void setConf(Configuration conf);

  String getConfPath();

  void setConfPath(String confPath);

  @JsonIgnore
  Map<String, String> getPropertyMap();

  void setPropertyMap(Map<String, String> propertyMap);
}
