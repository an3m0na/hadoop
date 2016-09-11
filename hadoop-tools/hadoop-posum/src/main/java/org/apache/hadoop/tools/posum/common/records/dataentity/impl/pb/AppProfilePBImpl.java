package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.PosumProtos.AppProfileProto;
import org.apache.hadoop.yarn.proto.PosumProtos.AppProfileProtoOrBuilder;

/**
 * Created by ane on 3/21/16.
 */
public class AppProfilePBImpl extends GeneralDataEntityPBImpl<AppProfile, AppProfileProto, AppProfileProto.Builder>
        implements AppProfile {

    @Override
    void initBuilder() {
        builder = viaProto ? AppProfileProto.newBuilder(proto) : AppProfileProto.newBuilder();
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
        if (!p.hasId())
            return null;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        if (id == null) {
            builder.clearId();
            return;
        }
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
        if (startTime == null) {
            builder.clearStartTime();
            return;
        }
        builder.setStartTime(startTime);
    }

    @Override
    public String getUser() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        if(!p.hasUser())
            return null;
        return p.getUser();
    }

    @Override
    public void setUser(String user) {
        maybeInitBuilder();
        if (user == null) {
            builder.clearUser();
            return;
        }
        builder.setUser(user);
    }

    @Override
    public String getName() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        if(!p.hasName())
            return null;
        return p.getName();
    }

    @Override
    public void setName(String name) {
        maybeInitBuilder();
        if (name == null) {
            builder.clearName();
            return;
        }
        builder.setName(name);
    }

    @Override
    public YarnApplicationState getState() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasState())
            return null;
        return YarnApplicationState.valueOf(p.getState().name().substring("STATE_".length()));
    }

    @Override
    public void setState(YarnApplicationState state) {
        maybeInitBuilder();
        if (state == null) {
            builder.clearState();
            return;
        }
        builder.setState(AppProfileProto.AppStateProto.valueOf("STATE_" + state.name()));
    }

    @Override
    public FinalApplicationStatus getStatus() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasStatus())
            return null;
        return FinalApplicationStatus.valueOf(p.getStatus().name().substring("STATUS_".length()));
    }

    @Override
    public void setStatus(FinalApplicationStatus status) {
        maybeInitBuilder();
        if (status == null) {
            builder.clearStatus();
            return;
        }
        builder.setStatus(AppProfileProto.AppStatusProto.valueOf("STATUS_" + status.name()));
    }

    @Override
    public Long getFinishTime() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getFinishTime();
    }

    @Override
    public void setFinishTime(Long finishTime) {
        maybeInitBuilder();
        if (finishTime == null) {
            builder.clearFinishTime();
            return;
        }
        builder.setFinishTime(finishTime);
    }

    @Override
    public RestClient.TrackingUI getTrackingUI() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasTrackingUI())
            return null;
        return RestClient.TrackingUI.valueOf(p.getTrackingUI().name().substring("UI_".length()));
    }

    @Override
    public void setTrackingUI(RestClient.TrackingUI trackingUI) {
        maybeInitBuilder();
        if (trackingUI == null) {
            builder.clearTrackingUI();
            return;
        }
        builder.setTrackingUI(AppProfileProto.AppTrackingUIProto.valueOf("UI_" + trackingUI.name()));
    }

    @Override
    public void setQueue(String queue) {
        maybeInitBuilder();
        if (queue == null) {
            builder.clearQueue();
            return;
        }
        builder.setQueue(queue);
    }

    @Override
    public String getQueue() {
        AppProfileProtoOrBuilder p = viaProto ? proto : builder;
        if(!p.hasQueue())
            return null;
        return p.getQueue();
    }
}
