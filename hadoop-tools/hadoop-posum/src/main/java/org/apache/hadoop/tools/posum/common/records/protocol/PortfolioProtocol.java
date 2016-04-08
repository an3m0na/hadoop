package org.apache.hadoop.tools.posum.common.records.protocol;

/**
 * Created by ane on 3/31/16.
 */
public interface PortfolioProtocol {
    long versionID = 1L;

    SimpleResponse configureScheduler(ConfigurationRequest request);
    SimpleResponse initScheduler(ConfigurationRequest request);
    SimpleResponse reinitScheduler(ConfigurationRequest request);
    HandleEventResponse handleSchedulerEvent(HandleEventRequest request);
}
