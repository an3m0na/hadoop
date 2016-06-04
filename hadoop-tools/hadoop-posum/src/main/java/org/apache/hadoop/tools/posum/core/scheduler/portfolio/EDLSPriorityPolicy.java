package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by ane on 1/22/16.
 */
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
        CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
        capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
        capacityConf.setQueues("root", new String[]{DEADLINE_QUEUE, BATCH_QUEUE});
        capacityConf.setCapacity("root." + DEADLINE_QUEUE, 50);
        capacityConf.setCapacity("root." + BATCH_QUEUE, 50);
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
            ParentQueue root = readField("root");
            for (Iterator<CSQueue> iter = root.getChildQueues().iterator(); iter.hasNext(); ) {
                CSQueue current = iter.next();
                iter.remove();
                root.getChildQueues().add(current);
            }
        }
        super.allocateContainersToNode(node);
    }
}

