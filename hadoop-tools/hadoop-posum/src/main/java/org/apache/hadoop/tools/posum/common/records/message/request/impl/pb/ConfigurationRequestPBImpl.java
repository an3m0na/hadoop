package org.apache.hadoop.tools.posum.common.records.message.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.yarn.proto.POSUMProtos.ConfigurationPropertiesProto;
import org.apache.hadoop.yarn.proto.YarnProtos;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public class ConfigurationRequestPBImpl extends SimpleRequestPBImpl<Map<String, String>> {


    @Override
    public ByteString payloadToBytes(final Map<String, String> payload) {
        if (payload == null)
            return ByteString.EMPTY;
        ConfigurationPropertiesProto.Builder builder = ConfigurationPropertiesProto.newBuilder();
        builder.addAllProperties(new Iterable<YarnProtos.StringStringMapProto>() {
            @Override
            public Iterator<YarnProtos.StringStringMapProto> iterator() {
                return new Iterator<YarnProtos.StringStringMapProto>() {

                    Iterator<Map.Entry<String, String>> iterator = payload.entrySet().iterator();

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public YarnProtos.StringStringMapProto next() {
                        Map.Entry<String, String> element = iterator.next();
                        YarnProtos.StringStringMapProto.Builder elementProtoBuilder =
                                YarnProtos.StringStringMapProto.newBuilder();
                        elementProtoBuilder.setKey(element.getKey());
                        elementProtoBuilder.setValue(element.getValue());
                        return null;
                    }

                    @Override
                    public void remove() {
                        throw new NotImplementedException();
                    }
                };
            }
        });
        return builder.build().toByteString();
    }

    @Override
    public Map<String, String> bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        ConfigurationPropertiesProto proto = ConfigurationPropertiesProto.parseFrom(data);
        Map<String, String> ret = new HashMap<>(proto.getPropertiesCount());
        for (YarnProtos.StringStringMapProto element : proto.getPropertiesList()) {
            ret.put(element.getKey(), element.getValue());
        }
        return ret;
    }
}
