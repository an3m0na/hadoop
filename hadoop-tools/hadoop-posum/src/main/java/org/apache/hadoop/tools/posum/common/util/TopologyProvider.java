package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.RackResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK;

public class TopologyProvider {

  private Map<String, String> topology;
  private Set<String> activeNodes;

  public TopologyProvider(Map<String, String> topology) {
    this.topology = new TreeMap<>(topology);
  }

  public TopologyProvider(Configuration conf) {
    RackResolver.init(conf);
  }

  public String resolve(String name) {
    String rack = getRack(name);
    return rack == null ? DEFAULT_RACK : rack + "/" + name;
  }

  public List<String> resolve(List<String> names) {
    List<String> ret = new ArrayList<>(names.size());
    for (String name : names) {
      ret.add(resolve(name));
    }
    return ret;
  }

  public List<String> getRacks(List<String> names) {
    List<String> ret = new ArrayList<>(names.size());
    for (String name : names) {
      ret.add(getRack(name));
    }
    return ret;
  }

  public String getRack(String name) {
    return topology == null ? RackResolver.resolve(name).getNetworkLocation() : topology.get(name);
  }

  public Set<String> getActiveNodes() {
    if (activeNodes == null || activeNodes.isEmpty())
      return topology.keySet();
    return activeNodes;
  }

  public void setActiveNodes(Set<String> activeNodes) {
    this.activeNodes = activeNodes;
  }
}
