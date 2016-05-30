package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.StringStringMapPayloadPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.JobConfProxyProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.JobConfProxyProtoOrBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 3/21/16.
 */
public class JobConfProxyPBImpl extends GeneralDataEntityPBImpl<JobConfProxy, JobConfProxyProto, JobConfProxyProto.Builder>
        implements JobConfProxy {

    @Override
    void initBuilder() {
        builder = viaProto ? JobConfProxyProto.newBuilder(proto) : JobConfProxyProto.newBuilder();
    }

    JobConf conf;

    @Override
    void buildProto() {
        maybeInitBuilder();
        if (conf != null) {
            Map<String, String> map = new HashMap<>(conf.size());
            for (Map.Entry<String, String> prop : conf) {
                map.put(prop.getKey(), prop.getValue());
            }
            StringStringMapPayloadPBImpl mapPayloadPB = new StringStringMapPayloadPBImpl();
            mapPayloadPB.setEntries(map);
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
        return "".equals(p.getId()) ? null : p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        if (id != null)
            builder.setId(id);
    }

    @Override
    public void setConf(JobConf conf) {
        this.conf = conf;

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
    public String getEntry(String name) {
        return getConf().get(name);
    }

    @Override
    public JobConf getConf() {
        if (this.conf == null) {
            JobConfProxyProtoOrBuilder p = viaProto ? proto : builder;
            this.conf = new JobConf(true);
            StringStringMapPayloadPBImpl mapPayloadPB = new StringStringMapPayloadPBImpl(p.getProperties());
            for (Map.Entry<String, String> prop : mapPayloadPB.getEntries().entrySet()) {
                conf.set(prop.getKey(), prop.getValue());
            }
        }
        return this.conf;
    }
}
