package org.apache.hadoop.tools.posum.scheduler.core;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;

interface MetaSchedulerCommService extends Service, MetaSchedulerProtocol, DatabaseProvider {

}
