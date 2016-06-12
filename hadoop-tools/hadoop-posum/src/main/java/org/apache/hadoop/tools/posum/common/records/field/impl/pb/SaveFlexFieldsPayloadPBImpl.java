package org.apache.hadoop.tools.posum.common.records.field.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DataEntityDBPBImpl;
import org.apache.hadoop.tools.posum.common.records.field.JobForAppPayload;
import org.apache.hadoop.tools.posum.common.records.field.SaveFlexFieldsPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos.SaveFlexFieldsPayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SaveFlexFieldsPayloadProtoOrBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public class SaveFlexFieldsPayloadPBImpl extends SaveFlexFieldsPayload {
    private SaveFlexFieldsPayloadProto proto = SaveFlexFieldsPayloadProto.getDefaultInstance();
    private SaveFlexFieldsPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    private Map<String, String> newFields;

    public SaveFlexFieldsPayloadPBImpl() {
        builder = SaveFlexFieldsPayloadProto.newBuilder();
    }

    public SaveFlexFieldsPayloadPBImpl(SaveFlexFieldsPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SaveFlexFieldsPayloadProto getProto() {
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
        if (newFields != null) {
            StringStringMapPayloadPBImpl mapPayloadPB = new StringStringMapPayloadPBImpl();
            mapPayloadPB.setEntries(newFields);
            builder.setNewFields(mapPayloadPB.getProto());
        }
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
            builder = SaveFlexFieldsPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityDB getEntityDB() {
        SaveFlexFieldsPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return new DataEntityDBPBImpl(p.getEntityDB());
    }

    @Override
    public void setEntityDB(DataEntityDB db) {
        maybeInitBuilder();
        builder.setEntityDB(((DataEntityDBPBImpl) db).getProto());
    }

    @Override
    public String getJobId() {
        SaveFlexFieldsPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getJobId();
    }

    @Override
    public void setJobId(String id) {
        maybeInitBuilder();
        builder.setJobId(id);
    }

    @Override
    public Map<String, String> getNewFields() {
        if (newFields == null) {
            SaveFlexFieldsPayloadProtoOrBuilder p = viaProto ? proto : builder;
            newFields = new StringStringMapPayloadPBImpl(p.getNewFields()).getEntries();
        }
        return newFields;
    }

    @Override
    public void setNewFields(Map<String, String> newFields) {
        this.newFields = newFields;
    }

    @Override
    public boolean getForHistory() {
        SaveFlexFieldsPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getForHistory();
    }

    @Override
    public void setForHistory(boolean forHistory) {
        maybeInitBuilder();
        builder.setForHistory(forHistory);
    }

}