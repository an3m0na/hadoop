package org.apache.hadoop.tools.posum.common.records.protocol;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.proto.POSUMProtos.ConfigurationRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.ConfigurationRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public class ConfigurationRequestPBImpl extends ConfigurationRequest {
    private ConfigurationRequestProto proto = ConfigurationRequestProto.getDefaultInstance();
    private ConfigurationRequestProto.Builder builder = null;
    private boolean viaProto = false;
    private Map<String, String> properties;

    public ConfigurationRequestPBImpl() {
        builder = ConfigurationRequestProto.newBuilder();
    }

    public ConfigurationRequestPBImpl(ConfigurationRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public ConfigurationRequestProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    @Override
    public int hashCode() {
        return getProto().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other.getClass().isAssignableFrom(this.getClass())) {
            return this.getProto().equals(this.getClass().cast(other).getProto());
        }
        return false;
    }

    @Override
    public String toString() {
        return TextFormat.shortDebugString(getProto());
    }

    private void mergeLocalToBuilder() {
        maybeInitBuilder();
        builder.clearProperties();
        if (properties == null)
            return;
        Iterable<YarnProtos.StringStringMapProto> iterable =
                new Iterable<YarnProtos.StringStringMapProto>() {

                    @Override
                    public Iterator<YarnProtos.StringStringMapProto> iterator() {
                        return new Iterator<YarnProtos.StringStringMapProto>() {

                            Iterator<String> keyIter = properties.keySet().iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public YarnProtos.StringStringMapProto next() {
                                String key = keyIter.next();
                                return YarnProtos.StringStringMapProto.newBuilder().setKey(key).setValue(
                                        (properties.get(key))).build();
                            }

                            @Override
                            public boolean hasNext() {
                                return keyIter.hasNext();
                            }
                        };
                    }
                };
        builder.addAllProperties(iterable);
    }

    private void mergeLocalToProto() {
        if (viaProto)
            maybeInitBuilder();
        mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = ConfigurationRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Map<String, String> getProperties() {
        if (properties == null) {
            ConfigurationRequestProtoOrBuilder p = viaProto ? proto : builder;
            List<YarnProtos.StringStringMapProto> list = p.getPropertiesList();
            properties = new HashMap<>();

            for (YarnProtos.StringStringMapProto c : list) {
                properties.put(c.getKey(), c.getValue());
            }
        }
        return properties;
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        if (properties == null)
            return;
        this.properties = new HashMap<>();
        this.properties.putAll(properties);
    }
}
