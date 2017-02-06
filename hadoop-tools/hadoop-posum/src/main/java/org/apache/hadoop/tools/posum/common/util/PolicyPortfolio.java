package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.*;

import java.util.HashMap;

public class PolicyPortfolio extends HashMap<String, Class<? extends PluginPolicy>>{

    public enum StandardPolicy {
        FIFO(FifoPolicy.class),
        DATA(DataOrientedPolicy.class),
        EDLS_SH(EDLSSharePolicy.class),
        EDLS_PR(EDLSPriorityPolicy.class),
        LOCF(LocalityFirstPolicy.class),
        SRTF(ShortestRTFirstPolicy.class);

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
                    if (entryParts.length != 2) {
                        Class<? extends PluginPolicy> implClass =
                                conf.getClass(entryParts[1], null, PluginPolicy.class);
                        if (implClass != null)
                            put(entryParts[0], implClass);
                        else
                            throw new PosumException("Invalid policy class " + entryParts[1]);
                    }
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

    public String getDefaultPolicyName() {
        return defaultPolicyName;
    }
}
