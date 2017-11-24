package org.apache.hadoop.tools.posum.common.util.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.common.records.call.CallUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.StringListPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.util.RackResolver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK;

public class TopologyProvider {

  private Map<String, String> topology;
  private Set<String> activeNodes;
  private DataStore dataStore;

  public TopologyProvider(Map<String, String> topology) {
    this.topology = new TreeMap<>(topology);
  }

  public TopologyProvider(Configuration conf, DataStore dataStore) {
    this.dataStore = dataStore;
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
    if (topology != null)
      return topology.keySet();
    LogEntry<StringListPayload> activeNodesLog = dataStore.execute(
      CallUtils.findStatReportCall(LogEntry.Type.ACTIVE_NODES), DatabaseReference.getLogs()).getEntity();
    if (activeNodesLog != null) {
      activeNodes = new HashSet<>(activeNodesLog.getDetails().getEntries());
    }
    if (activeNodes != null)
      return activeNodes;
    throw new PosumException("No active nodes are known");
  }

}
