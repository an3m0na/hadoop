package org.apache.hadoop.tools.posum.common.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.EDLSPriorityPolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.EDLSSharePolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.FifoPolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.LocalityFirstPolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;

import java.util.HashMap;

public class PolicyPortfolio extends HashMap<String, Class<? extends PluginPolicy>> {

  public enum StandardPolicy {
    FIFO(FifoPolicy.class),
    EDLS_SH(EDLSSharePolicy.class),
    EDLS_PR(EDLSPriorityPolicy.class),
    LOCF(LocalityFirstPolicy.class);//,
    //SRTF(ShortestRTFirstPolicy.class);

    Class<? extends PluginPolicy> implClass;

    StandardPolicy(Class<? extends PluginPolicy> implClass) {
      this.implClass = implClass;
    }
  }

  private String defaultPolicyName;

  public PolicyPortfolio() {

  }

  public PolicyPortfolio(Configuration conf) {
    super(StandardPolicy.values().length);
    String policyMap = conf.get(PosumConfiguration.SCHEDULER_POLICY_MAP);
    if (policyMap != null) {
      try {
        for (String entry : policyMap.split(",")) {
          String[] entryParts = entry.split("=");
          if (entryParts.length != 2)
            throw new PosumException("Invalid policy entry " + entry);
          put(entryParts[0], resolveImplClass(conf, entryParts[1]));
        }
      } catch (Exception e) {
        throw new PosumException("Could not parse policy map");
      }
    } else {
      for (StandardPolicy policy : StandardPolicy.values()) {
        put(policy.name(), policy.implClass);
      }
    }
    defaultPolicyName = conf.get(PosumConfiguration.DEFAULT_POLICY, PosumConfiguration.DEFAULT_POLICY_DEFAULT);
    if (get(defaultPolicyName) == null) {
      throw new PosumException("Invalid policy portfolio configuration. No default policy implementation");
    }
  }

  private Class<? extends PluginPolicy> resolveImplClass(Configuration conf, String name) throws ClassNotFoundException {
    Class<?> theClass = conf.getClassByName(name);
    if (!PluginPolicy.class.isAssignableFrom(theClass))
      throw new PosumException(theClass + " is not a PluginPolicy");
    return theClass.asSubclass(PluginPolicy.class);
  }

  public String getDefaultPolicyName() {
    return defaultPolicyName;
  }
}
