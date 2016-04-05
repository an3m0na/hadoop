package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;

/**
 * Created by ane on 4/5/16.
 */
public class DummyRMContext extends RMContextImpl {

    public DummyRMContext(Configuration yarnConfiguration){
        setYarnConfiguration(yarnConfiguration);
    }
}
