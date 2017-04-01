package org.apache.hadoop.tools.posum.client.data;

import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

import java.util.List;
import java.util.Map;


public interface DataStore {

  <T extends Payload> T execute(DatabaseCall<T> call, DatabaseReference db);

  Map<DatabaseReference, List<DataEntityCollection>> listCollections();

  void clear();

  void clearDatabase(DatabaseReference db);

  void copyDatabase(DatabaseReference sourceDB, DatabaseReference destinationDB);

  void copyCollection(DataEntityCollection collection, DatabaseReference sourceDB, DatabaseReference destinationDB);

}
