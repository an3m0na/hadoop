package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

public class EDLSPriorityPolicy extends EDLSPolicy<EDLSPriorityPolicy> {

  private static Log logger = LogFactory.getLog(EDLSPriorityPolicy.class);

  private String priorityQueue = DEADLINE_QUEUE;
  private Random random;

  public EDLSPriorityPolicy() {
    super(EDLSPriorityPolicy.class);
    random = new Random(System.currentTimeMillis());
  }

  @Override
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    CapacitySchedulerConfiguration capacityConf = super.loadCustomCapacityConf(conf);
    capacityConf.setCapacity(ROOT_QUEUE + DOT + DEADLINE_QUEUE, 50);
    capacityConf.setCapacity(ROOT_QUEUE + DOT + BATCH_QUEUE, 50);
    return capacityConf;
  }

  @Override
  public Comparator<CSQueue> getQueueComparator() {
    return new Comparator<CSQueue>() {
      @Override
      public int compare(CSQueue o1, CSQueue o2) {
        if (o1 instanceof LeafQueue && o2 instanceof LeafQueue) {
          LeafQueue leaf1 = (LeafQueue) o1, leaf2 = (LeafQueue) o2;
          // the current queue should always be first
          if (priorityQueue.equals(leaf1.getQueueName()))
            return -1;
          if (priorityQueue.equals(leaf2.getQueueName()))
            return 1;
        }
        // deafult order if not leaf queues; should never happen here
        if (o1.getUsedCapacity() < o2.getUsedCapacity())
          return -1;
        return 1;
      }
    };
  }

  @Override
  protected void allocateContainersToNode(FiCaSchedulerNode node) {
    float die = random.nextFloat();
    boolean refresh = false;
    if (die < deadlinePriority) {
      if (!priorityQueue.equals(DEADLINE_QUEUE)) {
        refresh = true;
        priorityQueue = DEADLINE_QUEUE;
      }
    } else {
      if (!priorityQueue.equals(BATCH_QUEUE)) {
        refresh = true;
        priorityQueue = BATCH_QUEUE;
      }
    }
    if (refresh) {
      // remove queues and add them again to refresh priority
      ParentQueue root = readField(ROOT_QUEUE);
      for (Iterator<CSQueue> iter = root.getChildQueues().iterator(); iter.hasNext(); ) {
        CSQueue current = iter.next();
        iter.remove();
        root.getChildQueues().add(current);
      }
    }
    super.allocateContainersToNode(node);
  }
}

