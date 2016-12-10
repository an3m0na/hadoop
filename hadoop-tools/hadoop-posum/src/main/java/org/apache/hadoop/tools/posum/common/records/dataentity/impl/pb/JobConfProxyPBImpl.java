package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.StringStringMapPayloadPBImpl;
import org.apache.hadoop.yarn.proto.PosumProtos.JobConfProxyProto;
import org.apache.hadoop.yarn.proto.PosumProtos.JobConfProxyProtoOrBuilder;

import java.util.HashMap;
import java.util.Map;

public class JobConfProxyPBImpl extends GeneralDataEntityPBImpl<JobConfProxy, JobConfProxyProto, JobConfProxyProto.Builder>
        implements JobConfProxy {

    public JobConfProxyPBImpl() {
    }

    public JobConfProxyPBImpl(JobConfProxyProto proto) {
        super(proto);
    }

    @Override
    void initBuilder() {
        builder = viaProto ? JobConfProxyProto.newBuilder(proto) : JobConfProxyProto.newBuilder();
    }

    private Configuration conf;
    private Map<String, String> propertyMap;

    @Override
    void buildProto() {
        maybeInitBuilder();
        propertyMap = getPropertyMap();
        if (propertyMap != null) {
            StringStringMapPayloadPBImpl mapPayloadPB = new StringStringMapPayloadPBImpl();
            mapPayloadPB.setEntries(propertyMap);
            builder.setProperties(mapPayloadPB.getProto());
        }
        proto = builder.build();
    }

    @Override
    public JobConfProxy parseToEntity(ByteString data) throws InvalidProtocolBufferException {
        this.proto = JobConfProxyProto.parseFrom(data);
        viaProto = true;
        return this;
    }

    @Override
    public String getId() {
        JobConfProxyProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasId())
            return null;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        if (id == null) {
            builder.clearId();
            return;
        }
        builder.setId(id);
    }

    @Override
    public JobConfProxy copy() {
        return new JobConfProxyPBImpl(getProto());
    }

    @Override
    public String getConfPath() {
        JobConfProxyProtoOrBuilder p = viaProto ? proto : builder;
        return p.getConfPath();
    }

    @Override
    public void setConfPath(String confPath) {
        maybeInitBuilder();
        if (confPath != null)
            builder.setConfPath(confPath);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        if (conf == null) {
            conf = new JobConf();
            for (Map.Entry<String, String> prop : getPropertyMap().entrySet()) {
                conf.set(prop.getKey(), prop.getValue());
            }
        }
        return conf;
    }

    public Map<String, String> getPropertyMap() {
        if (propertyMap == null) {
            if (conf != null) {
                propertyMap = new HashMap<>(conf.size());
                for (Map.Entry<String, String> prop : conf) {
                    propertyMap.put(prop.getKey(), prop.getValue());
                }
            } else {
                JobConfProxyProtoOrBuilder p = viaProto ? proto : builder;
                StringStringMapPayloadPBImpl mapPayloadPB = new StringStringMapPayloadPBImpl(p.getProperties());
                propertyMap = mapPayloadPB.getEntries();
            }
        }
        return propertyMap;
    }

    public void setPropertyMap(Map<String, String> propertyMap) {
        this.propertyMap = propertyMap;
    }

    @Override
    public String getEntry(String name) {
        if (conf != null)
            return conf.get(name);
        return getPropertyMap().get(name);
    }
}
