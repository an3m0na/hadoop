package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtCaSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtensibleCapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

import javax.xml.crypto.Data;
import java.util.Comparator;

public class DataOrientedPolicy extends ExtensibleCapacityScheduler<DOSAppAttempt, ExtCaSchedulerNode> {

  private static Log logger = LogFactory.getLog(DataOrientedPolicy.class);

  public DataOrientedPolicy() {
    super(DOSAppAttempt.class, ExtCaSchedulerNode.class, DataOrientedPolicy.class.getName(), true);
  }

  @Override
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
    capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
    return capacityConf;
  }

  @Override
  public Comparator<FiCaSchedulerApp> getApplicationComparator() {
    return new Comparator<FiCaSchedulerApp>() {
      @Override
      public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
        if (a1.getApplicationId()
          .equals(a2.getApplicationId()))
          return 0;
        DOSAppAttempt dosa1 = (DOSAppAttempt) a1;
        DOSAppAttempt dosa2 = (DOSAppAttempt) a2;
        if (dosa1.getTotalInputSize() == null)
          return 1;
        if (dosa2.getTotalInputSize() == null) {
          return -1;
        }
        return new Long(dosa1.getTotalInputSize() -
          dosa2.getTotalInputSize()).intValue();
      }
    };
  }

  @Override
  protected void updateAppPriority(DOSAppAttempt app) {
    logger.debug("Updating app priority");
    try {
      String appId = app.getApplicationId().toString();
      if (app.getTotalInputSize() != null)
        return;
      Database db = dbProvider.getDatabase();
      if (db != null) {
        JobForAppCall getJob = JobForAppCall.newInstance(appId, app.getUser());
        JobProfile job = db.execute(getJob).getEntity();
        if (job != null) {
          Long size = job.getTotalInputBytes();
          if (size != null && size > 0) {
            logger.debug("Read input size for " + appId + ": " + size);
            app.setInputSplits(job.getInputSplits());
            app.setTotalInputSize(size);
          }
        }
      }
    } catch (Exception e) {
      logger.debug("Could not update app priority for : " + app.getApplicationId(), e);
    }
  }
}

