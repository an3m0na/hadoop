package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK;

public class TopologyProvider implements DNSToSwitchMapping {

  private TreeMap<Long, Map<String, String>> topologySnapshots;

  public TopologyProvider(Map<Long, Map<String, String>> topologySnapshots) {
    this.topologySnapshots = new TreeMap<>(topologySnapshots);
  }

  public TopologyProvider() {
    this.topologySnapshots = new TreeMap<>();
    this.topologySnapshots.put(0L, new HashMap<String, String>());
  }

  @Override
  public List<String> resolve(List<String> names) {
    //TODO something smarter
    return resolve(0, names);
  }

  @Override
  public void reloadCachedMappings() {

  }

  @Override
  public void reloadCachedMappings(List<String> names) {

  }

  public void addSnapshot(long timestamp, Map<String, String> topologySnapshot) {
    topologySnapshots.put(timestamp, topologySnapshot);
  }

  public String resolve(long timestamp, String name) {
    return getSnapshot(timestamp).get(name);
  }

  public List<String> resolve(long timestamp, List<String> names) {
    List<String> ret = new ArrayList<>(names.size());
    for (String name : names) {
      String rack = getSnapshot(timestamp).get(name);
        ret.add(rack == null? DEFAULT_RACK : rack + "/" + name);
    }
    return ret;
  }

  public List<String> getRacks(long timestamp, List<String> names) {
    List<String> ret = new ArrayList<>(names.size());
    for (String name : names) {
      String rack = getSnapshot(timestamp).get(name);
      ret.add(rack == null? DEFAULT_RACK : rack);
    }
    return ret;
  }

  public Set<String> getActiveHosts(long timestamp) {
    return getSnapshot(timestamp).keySet();
  }

  public Map<String, String> getSnapshot(long timestamp) {
    Map<String, String> snapshot = topologySnapshots.floorEntry(timestamp).getValue();
    if (snapshot == null)
      throw new RuntimeException("No topology snapshot found for timestamp " + timestamp);
    return snapshot;
  }
}
