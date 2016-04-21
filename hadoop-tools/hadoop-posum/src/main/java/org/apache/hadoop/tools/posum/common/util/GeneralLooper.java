package org.apache.hadoop.tools.posum.common.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;

/**
 * Created by ane on 4/18/16.
 */
public abstract class GeneralLooper<T> extends AbstractService {

    private Log logger = LogFactory.getLog(getClass());

    protected Configuration conf;
    protected LoopThread loop;
    protected long sleepInterval;
    protected long lastRun;

    private class LoopThread extends Thread {
        private volatile boolean exit = false;

        void exit() {
            exit = true;
            interrupt();
        }

        @Override
        public void run() {
            long time;
            lastRun = System.currentTimeMillis();
            while (!exit) {
                try {
                    doAction();
                    time = lastRun + sleepInterval - System.currentTimeMillis();
                    System.out.println("escaped and waiting "+time);
                    if (time > 0)
                        sleep(time);
                } catch (InterruptedException e) {
                    logger.warn(e);
                }
                lastRun = System.currentTimeMillis();
            }
        }
    }

    protected GeneralLooper(Class<T> tClass) {
        super(tClass.getName());
        this.loop = new LoopThread();
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.conf = conf;
    }

    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
        loop.start();
    }

    @Override
    protected void serviceStop() throws Exception {
        loop.exit();
        super.serviceStop();
    }

    public void setSleepInterval(long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    protected abstract void doAction();
}
