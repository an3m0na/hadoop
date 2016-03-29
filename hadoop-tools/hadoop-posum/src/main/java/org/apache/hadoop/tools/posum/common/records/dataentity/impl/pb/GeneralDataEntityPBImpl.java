package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.codehaus.jackson.annotate.JsonProperty;
import org.mongojack.Id;

/**
 * Created by ane on 3/21/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class GeneralDataEntityPBImpl<
        E extends GeneralDataEntity,
        P extends com.google.protobuf.GeneratedMessage & com.google.protobuf.MessageOrBuilder,
        B extends com.google.protobuf.GeneratedMessage.Builder<B>>
        implements GeneralDataEntity {
    @Id
    @JsonProperty("_id")
    public String id; // empty; only here to solve Jackson mapping

    public GeneralDataEntityPBImpl() {
        initBuilder();
    }

    public GeneralDataEntityPBImpl(P proto) {
        this.proto = proto;
        viaProto = true;
    }

    protected P proto;
    protected B builder;
    protected boolean viaProto = false;

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

    protected void mergeLocalToProto() {
        if (viaProto)
            maybeInitBuilder();
        buildProto();
        viaProto = true;
    }

    protected void maybeInitBuilder() {
        if (viaProto || builder == null)
            initBuilder();
        viaProto = false;
    }

    @JsonIgnore
    public P getProto() {
        if (!viaProto)
            mergeLocalToProto();
        return proto;
    }

//    public String getId() {
//        return id;
//    }
//
//    public void setId(String id) {
//       this.id = id;
//    }

    public abstract E parseToEntity(ByteString data) throws InvalidProtocolBufferException;

    abstract void initBuilder();

    abstract void buildProto();
}
