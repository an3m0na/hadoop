package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.DataOrientedPolicy;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.FifoPolicy;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.PluginPolicy;

import java.util.HashMap;

/**
 * Created by ane on 4/20/16.
 */
public class PolicyMap extends HashMap<String, PolicyMap.PolicyInfo> {

    public class PolicyInfo {
        private Class<? extends PluginPolicy> pClass;
        private int usageNumber = 0;
        private long usageTime = 0L;
        private long lastStarted = 0;

        public PolicyInfo(Class<? extends PluginPolicy> pClass) {
            this.pClass = pClass;
        }

        public void start(Long now) {
            usageNumber++;
            lastStarted = now;
        }

        public void stop(Long now) {
            usageTime += now - lastStarted;
            lastStarted = 0;
        }

        public Class<? extends PluginPolicy> getImplClass() {
            return pClass;
        }

        public int getUsageNumber() {
            return usageNumber;
        }

        public long getUsageTime() {
            return usageTime;
        }
    }

    public enum AvailablePolicy {
        FIFO(FifoPolicy.class),
        DATA(DataOrientedPolicy.class);

        Class<? extends PluginPolicy> implClass;

        AvailablePolicy(Class<? extends PluginPolicy> implClass) {
            this.implClass = implClass;
        }
    }

    private PolicyInfo defaultPolicy;

    public PolicyMap(Configuration conf) {
        super(AvailablePolicy.values().length);
        String policyMap = conf.get(POSUMConfiguration.SCHEDULER_POLICY_MAP);
        if (policyMap != null) {
            try {
                for (String entry : policyMap.split(",")) {
                    String[] entryParts = entry.split("=");
                    if (entryParts.length != 2) {
                        Class<? extends PluginPolicy> implClass =
                                conf.getClass(entryParts[1], null, PluginPolicy.class);
                        if (implClass != null)
                            put(entryParts[0], new PolicyInfo(implClass));
                        else
                            throw new POSUMException("Invalid policy class " + entryParts[1]);
                    }
                }
            } catch (Exception e) {
                throw new POSUMException("Could not parse policy map");
            }
        } else {
            for (AvailablePolicy policy : AvailablePolicy.values()) {
                put(policy.name(), new PolicyInfo(policy.implClass));
            }
        }
        String defaultPolicyName = conf.get(POSUMConfiguration.DEFAULT_POLICY, POSUMConfiguration.DEFAULT_POLICY_DEFAULT);
        if (defaultPolicyName != null) {
            defaultPolicy = this.get(defaultPolicyName);
        }
    }

    public PolicyInfo getDefaultPolicy() {
        return defaultPolicy;
    }
}
