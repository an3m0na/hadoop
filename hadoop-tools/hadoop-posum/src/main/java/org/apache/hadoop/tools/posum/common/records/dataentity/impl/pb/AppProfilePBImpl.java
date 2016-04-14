package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.POSUMProtos.AppProfileProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.AppProfileProtoOrBuilder;

/**
 * Created by ane on 3/21/16.
 */
public class AppProfilePBImpl extends GeneralDataEntityPBImpl<AppProfile, AppProfileProto, AppProfileProto.Builder>
        implements AppProfile {

    @Override
    void initBuilder() {
        builder = viaProto? AppProfileProto.newBuilder(proto) : AppProfileProto.newBuilder();
    }

    @Override
    void buildProto() {
        proto = builder.build();
    }

    @Override
    public AppProfile parseToEntity(ByteString data) throws InvalidProtocolBufferException {
        this.proto = AppProfileProto.parseFrom(data);
        viaProto = true;
        return this;
    }

    @Override
    public String getId() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        return "".equals(p.getId())? null : p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        builder.setId(id);
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
        AppProfileProto.AppStateProto enumValue = p.getState();
        if (enumValue == null || enumValue.name().equals("STATE_NULL"))
            return null;
        return YarnApplicationState.valueOf(enumValue.name().substring("STATE_".length()));
    }

    @Override
    public void setState(String state) {
        maybeInitBuilder();
        if (state != null)
            builder.setState(AppProfileProto.AppStateProto.valueOf("STATE_" + state));
        else
            builder.setState(AppProfileProto.AppStateProto.STATE_NULL);
    }

    @Override
    public FinalApplicationStatus getStatus() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        AppProfileProto.AppStatusProto enumValue = p.getStatus();
        if (enumValue == null || enumValue.name().equals("STATUS_NULL"))
            return null;
        return FinalApplicationStatus.valueOf(enumValue.name().substring("STATUS_".length()));
    }

    @Override
    public void setStatus(String status) {
        maybeInitBuilder();
        if (status != null)
            builder.setStatus(AppProfileProto.AppStatusProto.valueOf("STATUS_" + status));
        else
            builder.setStatus(AppProfileProto.AppStatusProto.STATUS_NULL);
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
        AppProfileProto.AppTrackingUIProto enumValue = p.getTrackingUI();
        if (enumValue == null || enumValue.name().equals("UI_NULL"))
            return null;
        return RestClient.TrackingUI.valueOf(enumValue.name().substring("UI_".length()));
    }

    @Override
    public void setTrackingUI(String trackingUI) {
        maybeInitBuilder();
        if (trackingUI != null)
            builder.setTrackingUI(AppProfileProto.AppTrackingUIProto.valueOf("UI_" + trackingUI));
        else
            builder.setTrackingUI(AppProfileProto.AppTrackingUIProto.UI_NULL);
    }
}
