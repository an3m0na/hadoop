package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.CollectionMapPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.CompoundScorePayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.PropertyMapPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.MultiEntityPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.PolicyInfoMapPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.PolicyInfoPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimplePropertyPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SingleEntityPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.StringListPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.StringStringMapPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.TaskPredictionPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.VoidPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;

public enum PayloadType {
  VOID(VoidPayloadPBImpl.class),
  SIMPLE_PROPERTY(SimplePropertyPayloadPBImpl.class),
  SINGLE_ENTITY(SingleEntityPayloadPBImpl.class),
  MULTI_ENTITY(MultiEntityPayloadPBImpl.class),
  STRING_STRING_MAP(StringStringMapPayloadPBImpl.class),
  STRING_LIST(StringListPayloadPBImpl.class),
  TASK_PREDICTION(TaskPredictionPayloadPBImpl.class),
  POLICY_INFO_MAP(PolicyInfoMapPayloadPBImpl.class),
  POLICY_INFO(PolicyInfoPayloadPBImpl.class),
  COLLECTION_MAP(CollectionMapPayloadPBImpl.class),
  PROPERTY_MAP(PropertyMapPayloadPBImpl.class),
  COMPOUND_SCORE(CompoundScorePayloadPBImpl.class);

  private Class<? extends PayloadPB> implClass;

  PayloadType(Class<? extends PayloadPB> implClass) {
    this.implClass = implClass;
  }

  public Class<? extends PayloadPB> getImplClass() {
    return implClass;
  }
}
