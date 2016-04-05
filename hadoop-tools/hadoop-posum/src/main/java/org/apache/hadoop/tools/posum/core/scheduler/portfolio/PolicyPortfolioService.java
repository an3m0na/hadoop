package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.ConfigurationRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.PortfolioProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.core.master.POSUMMasterContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import java.io.IOException;

/**
 * Created by ane on 3/31/16.
 */
public class PolicyPortfolioService extends AbstractService implements PortfolioProtocol {

    POSUMMasterContext pmContext;
    Configuration conf;
    //TODO choose default some other way
    Class<? extends PluginScheduler> currentSchedulerClass = DataOrientedScheduler.class;
    PluginScheduler currentScheduler;

    public PolicyPortfolioService(POSUMMasterContext context) {
        super(PolicyPortfolioService.class.getName());
        this.pmContext = context;
        //TODO move somewhere else
        try {
            this.currentScheduler = currentSchedulerClass.newInstance();

        } catch (InstantiationException | IllegalAccessException e) {
            throw new POSUMException("Could not initialize plugin scheduler", e);
        }
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        this.conf = conf;
        this.currentScheduler.initializePlugin(conf);
    }

    @Override
    public SimpleResponse configureScheduler(ConfigurationRequest request) {
        this.currentScheduler.setConf(new DummyYarnConfiguration(request.getProperties()));
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SimpleResponse initScheduler(ConfigurationRequest request) {
        this.currentScheduler.init(new DummyYarnConfiguration(request.getProperties()));
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SimpleResponse reinitScheduler(ConfigurationRequest request) {
        try {
            Configuration yarnConf = new DummyYarnConfiguration(request.getProperties());
            RMContext rmContext = new DummyRMContext(yarnConf);

            this.currentScheduler.reinitialize(yarnConf, rmContext);
        } catch (IOException e) {
            return SimpleResponse.newInstance(false, "Reinitialization exception", e);
        }
        return SimpleResponse.newInstance(true);
    }
}
