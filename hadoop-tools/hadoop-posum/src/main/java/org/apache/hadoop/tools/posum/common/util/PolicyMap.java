package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.DataOrientedPolicy;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.FifoPolicy;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.PluginPolicy;

import java.util.HashMap;

/**
 * Created by ane on 4/20/16.
 */
public class PolicyMap extends HashMap<String, Class<? extends PluginPolicy>> {

    public enum AvailablePolicy {
        FIFO(FifoPolicy.class),
        DATA(DataOrientedPolicy.class);

        Class<? extends PluginPolicy> implClass;

        AvailablePolicy(Class<? extends PluginPolicy> implClass) {
            this.implClass = implClass;
        }
    }

    private Class<? extends PluginPolicy> defaultPolicyClass;

    public PolicyMap(Configuration conf) {
        super(AvailablePolicy.values().length);
        String policyMap = conf.get(POSUMConfiguration.SCHEDULER_POLICY_MAP);
        if (policyMap != null) {
            try {
                for (String entry : policyMap.split(",")) {
                    String[] entryParts = entry.split("=");
                    if (entryParts.length != 2)
                        put(entryParts[0],
                                (Class<? extends PluginPolicy>) getClass().getClassLoader().loadClass(entryParts[1]));
                }
            } catch (Exception e) {
                throw new POSUMException("Could not parse policy map");
            }
        } else {
            for (AvailablePolicy policy : AvailablePolicy.values()) {
                put(policy.name(), policy.implClass);
            }
        }
        String className = conf.get(POSUMConfiguration.DEFAULT_POLICY, POSUMConfiguration.DEFAULT_POLICY_DEFAULT);
        if (className != null)
            defaultPolicyClass = conf.getClass(className, AvailablePolicy.FIFO.implClass, PluginPolicy.class);
    }

    public Class<? extends PluginPolicy> getDefaultPolicyClass() {
        return defaultPolicyClass;
    }
}
