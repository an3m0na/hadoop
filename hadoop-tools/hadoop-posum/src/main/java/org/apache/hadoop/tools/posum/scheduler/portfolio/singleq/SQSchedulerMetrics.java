package org.apache.hadoop.tools.posum.scheduler.portfolio.singleq;

import com.codahale.metrics.Gauge;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerMetrics;

import java.lang.reflect.Constructor;

/**
 * Created by ane on 1/29/16.
 */
public class SQSchedulerMetrics extends SchedulerMetrics {

        public SQSchedulerMetrics() {
            super();
        }

    static <M extends SQSchedulerMetrics> M getInstance(Class<M> mClass) {
        try {
            Constructor<M> constructor = mClass.getConstructor();
            return constructor.newInstance();
        } catch (Exception e) {
            throw new PosumException("Failed to instantiate scheduler queue via default constructor" + e);
        }
    }

        @Override
        public void trackQueue(String queueName) {
            trackedQueues.add(queueName);
            SingleQueuePolicy dataOrientedScheduler = (SingleQueuePolicy) scheduler;
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
