package org.apache.hadoop.tools.posum.master.scheduler.data;

import com.codahale.metrics.Gauge;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerMetrics;

/**
 * Created by ane on 1/29/16.
 */
public class DOSchedulerMetrics extends SchedulerMetrics {

        public DOSchedulerMetrics() {
            super();
        }

        @Override
        public void trackQueue(String queueName) {
            trackedQueues.add(queueName);
            DataOrientedScheduler dataOrientedScheduler = (DataOrientedScheduler) scheduler;
            // for FifoScheduler, only DEFAULT_QUEUE
            // here the three parameters doesn't affect results
            final QueueInfo queue = dataOrientedScheduler.getQueueInfo(queueName, false, false);
            // track currentCapacity, maximumCapacity (always 1.0f)
            metrics.register("variable.queue." + queueName + ".currentcapacity",
                    new Gauge<Float>() {
                        @Override
                        public Float getValue() {
                            return queue.getCurrentCapacity();
                        }
                    }
            );
            metrics.register("variable.queue." + queueName + ".",
                    new Gauge<Float>() {
                        @Override
                        public Float getValue() {
                            return queue.getCurrentCapacity();
                        }
                    }
            );
        }
    }
