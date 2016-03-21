package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.POSUMException;
import org.apache.hadoop.tools.posum.common.records.profile.AppProfile;
import org.apache.hadoop.tools.posum.common.records.profile.impl.pb.AppProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleEntityResponse;
import org.apache.hadoop.tools.posum.database.store.DataCollection;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityResponseProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SingleEntityResponsePBImpl extends SingleEntityResponse {
    SingleEntityResponseProto proto = SingleEntityResponseProto.getDefaultInstance();
    SingleEntityResponseProto.Builder builder = null;
    boolean viaProto = false;

    public SingleEntityResponsePBImpl() {
        builder = SingleEntityResponseProto.newBuilder();
    }

    public SingleEntityResponsePBImpl(SingleEntityResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SingleEntityResponseProto getProto() {
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
            builder = SingleEntityResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataCollection getType() {
        SingleEntityResponseProtoOrBuilder p = viaProto ? proto : builder;
        return DataCollection.valueOf(p.getType().name().substring("TYPE_".length()));
    }

    @Override
    public void setType(DataCollection type) {
        maybeInitBuilder();
        builder.setType(POSUMProtos.EntityType.valueOf("TYPE_" + type.name()));
    }

    @Override
    public AppProfile getEntity() {
        SingleEntityResponseProtoOrBuilder p = viaProto ? proto : builder;
        if (p.getEntity() != null) {
            try {
                AppProfile ret = new AppProfilePBImpl(POSUMProtos.AppProfileProto.parseFrom(p.getEntity()));
                return ret;
            } catch (Exception e) {
                throw new POSUMException("Could not read object from byte string " + p.getEntity());
            }
        }
        return null;
    }

    @Override
    public void setEntity(AppProfile entity) {
        maybeInitBuilder();
        if (entity != null)
            builder.setEntity(((AppProfilePBImpl) entity).getProto().toByteString());
    }
}
