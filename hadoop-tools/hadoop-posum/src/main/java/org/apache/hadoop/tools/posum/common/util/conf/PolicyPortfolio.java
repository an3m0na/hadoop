package org.apache.hadoop.tools.posum.common.util.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.edls.EDLSPriorityPolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.edls.EDLSSharePolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.locf.LocalityFirstPolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.srtf.ShortestRTFirstPolicy;

import java.util.HashMap;

public class PolicyPortfolio extends HashMap<String, Class<? extends PluginPolicy>> {

  public enum StandardPolicy {
    SRTF(ShortestRTFirstPolicy.class),
    LOCF(LocalityFirstPolicy.class),
    EDLS_SH(EDLSSharePolicy.class),
    EDLS_PR(EDLSPriorityPolicy.class);

    Class<? extends PluginPolicy> implClass;

    StandardPolicy(Class<? extends PluginPolicy> implClass) {
      this.implClass = implClass;
    }

    public Class<? extends PluginPolicy> getImplClass() {
      return implClass;
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
        throw new PosumException("Could not parse policy map", e);
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
