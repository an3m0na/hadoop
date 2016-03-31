package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.ConfigurationRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.PortfolioProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.SimpleResponse;
import org.apache.hadoop.tools.posum.core.master.POSUMMasterContext;

/**
 * Created by ane on 3/31/16.
 */
public class PolicyPortfolioService extends AbstractService implements PortfolioProtocol {

    POSUMMasterContext pmContext;
    Configuration conf;

    public PolicyPortfolioService(POSUMMasterContext context) {
        super(PolicyPortfolioService.class.getName());
        this.pmContext = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        this.conf = conf;
    }

    @Override
    public SimpleResponse configureScheduler(ConfigurationRequest request) {
        return null;
    }

    @Override
    public SimpleResponse initScheduler(ConfigurationRequest request) {
        return null;
    }

    @Override
    public SimpleResponse reinitScheduler(ConfigurationRequest request) {
        return null;
    }
}
