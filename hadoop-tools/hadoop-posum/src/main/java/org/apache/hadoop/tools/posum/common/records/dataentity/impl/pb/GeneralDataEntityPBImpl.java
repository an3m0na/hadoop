package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;

/**
 * Created by ane on 3/21/16.
 */
public interface GeneralDataEntityPBImpl<E extends GeneralDataEntity, T extends com.google.protobuf.GeneratedMessage> {
    T getProto();

    E parseToEntity(ByteString data) throws InvalidProtocolBufferException;
}
