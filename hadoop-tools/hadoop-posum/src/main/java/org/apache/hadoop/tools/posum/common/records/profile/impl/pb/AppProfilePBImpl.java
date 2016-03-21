package org.apache.hadoop.tools.posum.common.records.profile.impl.pb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.tools.posum.common.records.profile.AppProfile;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.POSUMProtos.AppProfileProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.AppProfileProtoOrBuilder;

/**
 * Created by ane on 3/21/16.
 */
public class AppProfilePBImpl extends AppProfile {
    private AppProfileProto proto = AppProfileProto.getDefaultInstance();
    private AppProfileProto.Builder builder = null;
    private boolean viaProto = false;

    public AppProfilePBImpl() {
        builder = AppProfileProto.newBuilder();
    }

    public AppProfilePBImpl(AppProfileProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    @JsonIgnore
    public AppProfileProto getProto() {
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
            builder = AppProfileProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Long getStartTime() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getStartTime();
    }

    @Override
    public void setStartTime(Long startTime) {
        maybeInitBuilder();
        builder.setStartTime(startTime);
    }

    @Override
    public String getUser() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getUser();
    }

    @Override
    public void setUser(String user) {
        maybeInitBuilder();
        builder.setUser(user);
    }

    @Override
    public String getName() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getName();
    }

    @Override
    public void setName(String name) {
        maybeInitBuilder();
        builder.setName(name);
    }

    @Override
    public YarnApplicationState getState() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        AppProfileProto.AppState enumValue = p.getState();
        if (enumValue == null || enumValue.name().equals("STATE_NULL"))
            return null;
        return YarnApplicationState.valueOf(enumValue.name().substring("STATE_".length()));
    }

    @Override
    public void setState(String state) {
        maybeInitBuilder();
        if (state != null)
            builder.setState(AppProfileProto.AppState.valueOf("STATE_" + state));
        else
            builder.setState(AppProfileProto.AppState.STATE_NULL);
    }

    @Override
    public FinalApplicationStatus getStatus() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        AppProfileProto.AppStatus enumValue = p.getStatus();
        if (enumValue == null || enumValue.name().equals("STATUS_NULL"))
            return null;
        return FinalApplicationStatus.valueOf(enumValue.name().substring("STATUS_".length()));
    }

    @Override
    public void setStatus(String status) {
        maybeInitBuilder();
        if (status != null)
            builder.setStatus(AppProfileProto.AppStatus.valueOf("STATUS_" + status));
        else
            builder.setStatus(AppProfileProto.AppStatus.STATUS_NULL);
    }

    @Override
    public Long getFinishTime() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getFinishTime();
    }

    @Override
    public void setFinishTime(Long finishTime) {
        maybeInitBuilder();
        builder.setFinishTime(finishTime);
    }

    @Override
    public RestClient.TrackingUI getTrackingUI() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        AppProfileProto.AppTrackingUI enumValue = p.getTrackingUI();
        if (enumValue == null || enumValue.name().equals("UI_NULL"))
            return null;
        return RestClient.TrackingUI.valueOf(enumValue.name().substring("UI_".length()));
    }

    @Override
    public void setTrackingUI(String trackingUI) {
        maybeInitBuilder();
        if (trackingUI != null)
            builder.setTrackingUI(AppProfileProto.AppTrackingUI.valueOf("UI_" + trackingUI));
        else
            builder.setTrackingUI(AppProfileProto.AppTrackingUI.UI_NULL);
    }

    @Override
    public String getId() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        builder.setId(id);
    }
}
