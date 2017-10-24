package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DatabaseReferencePBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.CollectionMapPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.CollectionEntryProto;
import org.apache.hadoop.yarn.proto.PosumProtos.CollectionMapPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.CollectionMapPayloadProtoOrBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CollectionMapPayloadPBImpl extends CollectionMapPayload implements PayloadPB {
  private CollectionMapPayloadProto proto = CollectionMapPayloadProto.getDefaultInstance();
  private CollectionMapPayloadProto.Builder builder = null;
  private boolean viaProto = false;

  private Map<DatabaseReference, List<DataEntityCollection>> entries;

  public CollectionMapPayloadPBImpl() {
    builder = CollectionMapPayloadProto.newBuilder();
  }

  public CollectionMapPayloadPBImpl(CollectionMapPayloadProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public CollectionMapPayloadProto getProto() {
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
    if (entries == null)
      return;
    builder.clearEntries();
    Iterable<CollectionEntryProto> iterable =
      new Iterable<CollectionEntryProto>() {

        @Override
        public Iterator<CollectionEntryProto> iterator() {
          return new Iterator<CollectionEntryProto>() {

            Iterator<Map.Entry<DatabaseReference, List<DataEntityCollection>>> entryIterator =
              entries.entrySet().iterator();

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public CollectionEntryProto next() {
              Map.Entry<DatabaseReference, List<DataEntityCollection>> mapEntry = entryIterator.next();
              CollectionEntryProto.Builder builder = CollectionEntryProto.newBuilder()
                .setDb(((DatabaseReferencePBImpl) mapEntry.getKey()).getProto());
              if (mapEntry.getValue() != null) {
                for (DataEntityCollection collection : mapEntry.getValue()) {
                  builder.addCollections(PosumProtos.EntityCollectionProto.valueOf("COLL_" + collection.name()));
                }
              }
              return builder.build();
            }

            @Override
            public boolean hasNext() {
              return entryIterator.hasNext();
            }
          };
        }
      };
    builder.addAllEntries(iterable);
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
      builder = CollectionMapPayloadProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Map<DatabaseReference, List<DataEntityCollection>> getEntries() {
    if (entries == null) {
      CollectionMapPayloadProtoOrBuilder p = viaProto ? proto : builder;
      entries = new HashMap<>(p.getEntriesCount());
      for (CollectionEntryProto entryProto : p.getEntriesList()) {
        List<DataEntityCollection> collections = new ArrayList<>(entryProto.getCollectionsCount());
        for (PosumProtos.EntityCollectionProto collectionProto : entryProto.getCollectionsList()) {
          collections.add(DataEntityCollection.valueOf(collectionProto.name().substring("COLL_".length())));
        }
        entries.put(new DatabaseReferencePBImpl(entryProto.getDb()), collections);
      }
    }
    return entries;
  }

  @Override
  public void setEntries(Map<DatabaseReference, List<DataEntityCollection>> entries) {
    maybeInitBuilder();
    if(entries == null){
      builder.clearEntries();
    }
    this.entries = entries;
  }

  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = CollectionMapPayloadProto.parseFrom(data);
    viaProto = true;
  }

}