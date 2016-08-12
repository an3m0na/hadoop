package org.apache.hadoop.tools.posum.common.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.scheduler.portfolio.*;

import java.util.HashMap;

/**
 * Created by ane on 4/20/16.
 */
//TODO refactor everything to a PayloadPBImpl
public class PolicyMap extends HashMap<String, PolicyMap.PolicyInfo> implements PayloadPB{

    @Override
    public ByteString getProtoBytes() {
        throw new NotImplementedException();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        throw new NotImplementedException();
    }

    public static class PolicyInfo {
        @JsonIgnore
        private Class<? extends PluginPolicy> pClass;

        private int usageNumber = 0;
        private long usageTime = 0L;
        private long lastStarted = 0L;

        public PolicyInfo() {

        }

        public PolicyInfo(Class<? extends PluginPolicy> pClass) {
            this.pClass = pClass;
        }

        public void start(Long now) {
            if (lastStarted != 0) {
                usageTime += now - lastStarted;
            }
            lastStarted = now;
            usageNumber++;


        }

        public void stop(Long now) {
            usageTime += now - lastStarted;
            lastStarted = 0;
        }

        @JsonIgnore
        public Class<? extends PluginPolicy> getImplClass() {
            return pClass;
        }

        public int getUsageNumber() {
            return usageNumber;
        }

        public long getUsageTime() {
            return usageTime;
        }

        public long getLastStarted() {
            return lastStarted;
        }

        public void setUsageNumber(int usageNumber) {
            this.usageNumber = usageNumber;
        }

        public void setUsageTime(long usageTime) {
            this.usageTime = usageTime;
        }

        public void setLastStarted(long lastStarted) {
            this.lastStarted = lastStarted;
        }
    }

    public enum AvailablePolicy {
        FIFO(FifoPolicy.class),
        DATA(DataOrientedPolicy.class),
        EDLS_SH(EDLSSharePolicy.class),
        EDLS_PR(EDLSPriorityPolicy.class),
        LOCF(LocalityFirstPolicy.class),
        SRTF(ShortestRTFirstPolicy.class);

        Class<? extends PluginPolicy> implClass;

        AvailablePolicy(Class<? extends PluginPolicy> implClass) {
            this.implClass = implClass;
        }
    }

    @JsonIgnore
    private PolicyInfo defaultPolicy;

    private String defaultPolicyName;
    private long schedulingStart = 0;
    private long totalChanges = 0;
    private String lastUsed;

    public PolicyMap() {

    }

    public PolicyMap(Configuration conf) {
        super(AvailablePolicy.values().length);
        String policyMap = conf.get(PosumConfiguration.SCHEDULER_POLICY_MAP);
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
                            throw new PosumException("Invalid policy class " + entryParts[1]);
                    }
                }
            } catch (Exception e) {
                throw new PosumException("Could not parse policy map");
            }
        } else {
            for (AvailablePolicy policy : AvailablePolicy.values()) {
                put(policy.name(), new PolicyInfo(policy.implClass));
            }
        }
        defaultPolicyName = conf.get(PosumConfiguration.DEFAULT_POLICY, PosumConfiguration.DEFAULT_POLICY_DEFAULT);
        if (defaultPolicyName != null) {
            defaultPolicy = this.get(defaultPolicyName);
        }
    }

    public PolicyInfo getDefaultPolicy() {
        return defaultPolicy;
    }

    public String getDefaultPolicyName() {
        return defaultPolicyName;
    }

    public long getTotalChanges() {
        return totalChanges;
    }

    public void incTotalChanges(long newChanges) {
        this.totalChanges += newChanges;
    }

    public void setTotalChanges(long totalChanges) {
        this.totalChanges = totalChanges;
    }

    public String getLastUsed() {
        return lastUsed;
    }

    public void setLastUsed(String lastUsed) {
        this.lastUsed = lastUsed;
    }

    public long getSchedulingStart() {
        return schedulingStart;
    }

    public void setSchedulingStart(long schedulingStart) {
        this.schedulingStart = schedulingStart;
    }
}
