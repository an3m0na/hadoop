package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
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
  }

  @Override
  public List<String> resolve(List<String> names) {
    List<String> ret = new ArrayList<>(names.size());
    for (String name : names)
      ret.add(DEFAULT_RACK + "/" + name);
    return ret;
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
