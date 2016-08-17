package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.GeneralDataEntityPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.MultiEntityPayload;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.MultiEntityPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.MultiEntityPayloadProtoOrBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public class MultiEntityPayloadPBImpl extends MultiEntityPayload implements PayloadPB {
    private MultiEntityPayloadProto proto = MultiEntityPayloadProto.getDefaultInstance();
    private MultiEntityPayloadProto.Builder builder = null;
    private boolean viaProto = false;
    private List<GeneralDataEntity> entities;

    public MultiEntityPayloadPBImpl() {
        builder = MultiEntityPayloadProto.newBuilder();
    }

    public MultiEntityPayloadPBImpl(MultiEntityPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public MultiEntityPayloadProto getProto() {
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
        builder.clearEntities();
        if (entities == null)
            return;
        Iterable<ByteString> iterable =
                new Iterable<ByteString>() {

                    @Override
                    public Iterator<ByteString> iterator() {
                        return new Iterator<ByteString>() {

                            Iterator<GeneralDataEntity> entityIterator = entities.iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public ByteString next() {
                                GeneralDataEntityPBImpl entityImpl = (GeneralDataEntityPBImpl) entityIterator.next();
                                if (entityImpl != null)
                                    return entityImpl.getProto().toByteString();
                                return ByteString.EMPTY;
                            }

                            @Override
                            public boolean hasNext() {
                                return entityIterator.hasNext();
                            }
                        };
                    }
                };
        builder.addAllEntities(iterable);
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
            builder = MultiEntityPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityCollection getEntityCollection() {
        MultiEntityPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasCollection())
            return null;
        return DataEntityCollection.valueOf(p.getCollection().name().substring("COLL_".length()));
    }

    @Override
    public void setEntityCollection(DataEntityCollection type) {
        maybeInitBuilder();
        if (type == null) {
            builder.clearCollection();
            return;
        }
        builder.setCollection(PosumProtos.EntityCollectionProto.valueOf("COLL_" + type.name()));
    }

    @Override
    public List<GeneralDataEntity> getEntities() {
        if (entities == null) {
            MultiEntityPayloadProtoOrBuilder p = viaProto ? proto : builder;
            entities = new ArrayList<>(p.getEntitiesCount());
            for (ByteString entityString : p.getEntitiesList()) {
                if (entityString.isEmpty())
                    continue;
                try {
                    Class eClass = getEntityCollection().getMappedClass();
                    entities.add(((GeneralDataEntityPBImpl) eClass.newInstance()).parseToEntity(entityString));
                } catch (Exception e) {
                    throw new PosumException("Could not read object from byte string " + entityString, e);
                }

            }
        }
        return entities;
    }

    @Override
    public void setEntities(List<GeneralDataEntity> entities) {
        maybeInitBuilder();
        this.entities = entities;
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = MultiEntityPayloadProto.parseFrom(data);
        viaProto = true;
    }
}
