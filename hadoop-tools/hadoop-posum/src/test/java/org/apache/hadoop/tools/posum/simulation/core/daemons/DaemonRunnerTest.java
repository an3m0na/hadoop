package org.apache.hadoop.tools.posum.simulation.core.daemons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.tools.posum.test.Utils.JOB1;
import static org.apache.hadoop.tools.posum.test.Utils.JOB1_ID;
import static org.apache.hadoop.tools.posum.test.Utils.JOB2;
import static org.apache.hadoop.tools.posum.test.Utils.JOB2_ID;
import static org.apache.hadoop.tools.posum.test.Utils.NODE1;
import static org.apache.hadoop.tools.posum.test.Utils.NODE2;
import static org.apache.hadoop.tools.posum.test.Utils.RACK1;
import static org.apache.hadoop.tools.posum.test.Utils.TASK11;
import static org.apache.hadoop.tools.posum.test.Utils.TASK12;
import static org.apache.hadoop.tools.posum.test.Utils.TASK21;
import static org.apache.hadoop.tools.posum.test.Utils.TASK22;

@Category(IntegrationTest.class)
public class DaemonRunnerTest {
  @Test
  public void testDaemons() throws Exception {
    Configuration conf = PosumConfiguration.newInstance();
    String schedulerClass = FifoScheduler.class.getName();
    Set<String> nodeSet = new HashSet<>(Arrays.asList(NODE1, NODE2));
    Map<String, String> racks = new HashMap<>(2);
    racks.put(NODE1, RACK1);
    racks.put(NODE2, RACK1);
    List<JobProfile> jobs = Arrays.asList(JOB1, JOB2);
    Map<String, List<TaskProfile>> tasks = new HashMap<>(1);
    tasks.put(JOB1_ID.toString(), Arrays.asList(TASK11, TASK12));
    tasks.put(JOB2_ID.toString(), Arrays.asList(TASK21, TASK22));

    new DaemonRunner(conf, schedulerClass, nodeSet, racks, jobs, tasks).start();
  }
}
