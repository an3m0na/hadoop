package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.util.TopologyProvider;
import org.apache.hadoop.tools.posum.data.mock.data.MockDataStoreImpl;
import org.apache.hadoop.tools.posum.scheduler.portfolio.ShortestRTFirstPolicy;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.tools.posum.test.Utils.JOB1;
import static org.apache.hadoop.tools.posum.test.Utils.JOB2;
import static org.apache.hadoop.tools.posum.test.Utils.NODE1;
import static org.apache.hadoop.tools.posum.test.Utils.NODE2;
import static org.apache.hadoop.tools.posum.test.Utils.RACK1;
import static org.apache.hadoop.tools.posum.test.Utils.TASK11;
import static org.apache.hadoop.tools.posum.test.Utils.TASK12;
import static org.apache.hadoop.tools.posum.test.Utils.TASK21;
import static org.apache.hadoop.tools.posum.test.Utils.TASK22;

@Category(IntegrationTest.class)
public class SimulationRunnerTest {
  @Test
  public void testDaemons() throws Exception {
    Map<String, String> racks = new HashMap<>(2);
    racks.put(NODE1, RACK1);
    racks.put(NODE2, RACK1);
    Database db = Database.from(new MockDataStoreImpl(), DatabaseReference.get(DatabaseReference.Type.SIMULATION, "runnerTest"));
    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(StoreAllCall.newInstance(DataEntityCollection.JOB, Arrays.asList(JOB1, JOB2)))
      .addCall(StoreAllCall.newInstance(DataEntityCollection.TASK, Arrays.asList(TASK11, TASK12)))
      .addCall(StoreAllCall.newInstance(DataEntityCollection.TASK, Arrays.asList(TASK21, TASK22)));
    db.execute(transaction);

    SimulationContext context = new SimulationContext();
    context.setDatabase(db);
    context.setSchedulerClass(ShortestRTFirstPolicy.class);
    context.setTopologyProvider(new TopologyProvider(Collections.singletonMap(0L, racks)));
    new SimulationRunner(context).run();
  }
}
