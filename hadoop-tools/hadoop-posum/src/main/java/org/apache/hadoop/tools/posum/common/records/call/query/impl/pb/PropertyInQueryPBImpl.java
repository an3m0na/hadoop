package org.apache.hadoop.tools.posum.common.records.call.query.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyInQuery;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimplePropertyPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.SimplePropertyPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyInQueryProto;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyInQueryProtoOrBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public class PropertyInQueryPBImpl extends PropertyInQuery implements PayloadPB {
    private PropertyInQueryProto proto = PropertyInQueryProto.getDefaultInstance();
    private PropertyInQueryProto.Builder builder = null;
    private boolean viaProto = false;

    private List<Object> values;

    public PropertyInQueryPBImpl() {
        builder = PropertyInQueryProto.newBuilder();
    }

    public PropertyInQueryPBImpl(PropertyInQueryProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public PropertyInQueryProto getProto() {
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
        builder.clearValues();
        if (values == null)
            return;
        final Iterable<SimplePropertyPayloadProto> iterable =
                new Iterable<SimplePropertyPayloadProto>() {
                    @Override
                    public Iterator<SimplePropertyPayloadProto> iterator() {
                        return new Iterator<SimplePropertyPayloadProto>() {

                            Iterator<Object> iterator = values.iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public SimplePropertyPayloadProto next() {
                                SimplePropertyPayload propertyValue =
                                        SimplePropertyPayload.newInstance("", iterator.next());
                                return ((SimplePropertyPayloadPBImpl) propertyValue).getProto();
                            }

                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }
                        };
                    }
                };
        builder.addAllValues(iterable);
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
            builder = PropertyInQueryProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getPropertyName() {
        PropertyInQueryProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasPropertyName())
            return null;
        return p.getPropertyName();
    }

    @Override
    public void setPropertyName(String propertyName) {
        maybeInitBuilder();
        if (propertyName == null) {
            builder.clearPropertyName();
            return;
        }
        builder.setPropertyName(propertyName);
    }

    @Override
    public List<Object> getValues() {
        if (values == null) {
            PropertyInQueryProtoOrBuilder p = viaProto ? proto : builder;
            values = new ArrayList<>(p.getValuesCount());
            for (SimplePropertyPayloadProto propertyProto : p.getValuesList()) {
                values.add(new SimplePropertyPayloadPBImpl(propertyProto).getValue());
            }
        }
        return values;
    }

    @Override
    public void setValues(List<Object> values) {
        maybeInitBuilder();
        this.values = values;
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = PropertyInQueryProto.parseFrom(data);
        viaProto = true;
    }
}
