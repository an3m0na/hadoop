package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

public abstract class DatabaseAlterationPayload implements Payload {

  public static DatabaseAlterationPayload newInstance(DatabaseReference db) {
    DatabaseAlterationPayload payload = Records.newRecord(DatabaseAlterationPayload.class);
    payload.setSourceDB(db);
    return payload;
  }

  public static DatabaseAlterationPayload newInstance(DatabaseReference sourceDB, DatabaseReference destinationDB) {
    DatabaseAlterationPayload payload = Records.newRecord(DatabaseAlterationPayload.class);
    payload.setSourceDB(sourceDB);
    payload.setDestinationDB(destinationDB);
    return payload;
  }

  public static DatabaseAlterationPayload newInstance(DatabaseReference sourceDB,
                                                      DatabaseReference destinationDB,
                                                      List<DataEntityCollection> collections) {
    DatabaseAlterationPayload payload = Records.newRecord(DatabaseAlterationPayload.class);
    payload.setTargetCollections(collections);
    payload.setSourceDB(sourceDB);
    payload.setDestinationDB(destinationDB);
    return payload;
  }

  public abstract DatabaseReference getSourceDB();

  public abstract void setSourceDB(DatabaseReference db);

  public abstract DatabaseReference getDestinationDB();

  public abstract void setDestinationDB(DatabaseReference db);

  public abstract List<DataEntityCollection> getTargetCollections();

  public abstract void setTargetCollections(List<DataEntityCollection> collection);
}
