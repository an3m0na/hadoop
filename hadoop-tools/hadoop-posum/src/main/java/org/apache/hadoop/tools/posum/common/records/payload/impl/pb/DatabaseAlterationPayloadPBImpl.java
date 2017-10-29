package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DatabaseReferencePBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.DatabaseAlterationPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseAlterationPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseAlterationPayloadProtoOrBuilder;
import org.apache.hadoop.yarn.proto.PosumProtos.EntityCollectionProto;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DatabaseAlterationPayloadPBImpl extends DatabaseAlterationPayload implements PayloadPB {
  private DatabaseAlterationPayloadProto proto = DatabaseAlterationPayloadProto.getDefaultInstance();
  private DatabaseAlterationPayloadProto.Builder builder = null;
  private boolean viaProto = false;
  private List<DataEntityCollection> collections;

  public DatabaseAlterationPayloadPBImpl() {
    builder = DatabaseAlterationPayloadProto.newBuilder();
  }

  public DatabaseAlterationPayloadPBImpl(DatabaseAlterationPayloadProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public DatabaseAlterationPayloadProto getProto() {
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
    if (collections == null)
      return;
    builder.clearTargetCollections();
    Iterable<EntityCollectionProto> iterable =
      new Iterable<EntityCollectionProto>() {

        @Override
        public Iterator<EntityCollectionProto> iterator() {
          return new Iterator<EntityCollectionProto>() {

            Iterator<DataEntityCollection> collectionIterator = collections.iterator();

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public EntityCollectionProto next() {
              DataEntityCollection collection = collectionIterator.next();
              return EntityCollectionProto.valueOf("COLL_" + collection.name());
            }

            @Override
            public boolean hasNext() {
              return collectionIterator.hasNext();
            }
          };
        }
      };
    builder.addAllTargetCollections(iterable);
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
      builder = DatabaseAlterationPayloadProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public DatabaseReference getSourceDB() {
    DatabaseAlterationPayloadProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSource())
      return null;
    return new DatabaseReferencePBImpl(p.getSource());
  }

  @Override
  public void setSourceDB(DatabaseReference db) {
    maybeInitBuilder();
    if (db == null) {
      builder.clearSource();
      return;
    }
    builder.setSource(((DatabaseReferencePBImpl) db).getProto());
  }

  @Override
  public DatabaseReference getDestinationDB() {
    DatabaseAlterationPayloadProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDestination())
      return null;
    return new DatabaseReferencePBImpl(p.getDestination());
  }

  @Override
  public void setDestinationDB(DatabaseReference db) {
    maybeInitBuilder();
    if (db == null) {
      builder.clearDestination();
      return;
    }
    builder.setDestination(((DatabaseReferencePBImpl) db).getProto());
  }

  @Override
  public List<DataEntityCollection> getTargetCollections() {
    if (collections == null) {
      DatabaseAlterationPayloadProtoOrBuilder p = viaProto ? proto : builder;
      collections = new ArrayList<>(p.getTargetCollectionsCount());
      for (EntityCollectionProto collectionProto : p.getTargetCollectionsList())
        collections.add(DataEntityCollection.valueOf(collectionProto.name().substring("COLL_".length())));
    }
    return collections;
  }

  @Override
  public void setTargetCollections(List<DataEntityCollection> collections) {
    maybeInitBuilder();
    if (collections == null) {
      builder.clearTargetCollections();
      this.collections = null;
      return;
    }
    this.collections = new ArrayList<>(collections.size());
    this.collections.addAll(collections);
  }


  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = DatabaseAlterationPayloadProto.parseFrom(data);
    viaProto = true;
  }
}
