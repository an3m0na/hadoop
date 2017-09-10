package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

public abstract class SaveJobFlexFieldsCall extends LockBasedDatabaseCallImpl<VoidPayload> {

  public static SaveJobFlexFieldsCall newInstance(String jobId, Map<String, String> newFields, boolean forHistory) {
    SaveJobFlexFieldsCall call = Records.newRecord(SaveJobFlexFieldsCall.class);
    call.setJobId(jobId);
    call.setNewFields(newFields);
    call.setForHistory(forHistory);
    return call;
  }

  public abstract String getJobId();

  public abstract void setJobId(String id);

  public abstract Map<String, String> getNewFields();

  public abstract void setNewFields(Map<String, String> newFields);

  public abstract boolean getForHistory();

  public abstract void setForHistory(boolean forHistory);

  @Override
  public VoidPayload execute(LockBasedDataStore dataStore, DatabaseReference db) {
    DataEntityCollection type = getForHistory() ? DataEntityCollection.JOB_HISTORY : DataEntityCollection.JOB;
    JobProfile job = dataStore.<JobProfile>findById(db, type, getJobId());
    if (job == null)
      throw new PosumException("Could not find job to save flex-fields: " + getJobId());
    job.addAllFlexFields(getNewFields());
    dataStore.updateOrStore(db, type, job);
    return VoidPayload.newInstance();
  }

  @Override
  public void lockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
    dataStore.lockForWrite(db);
  }

  @Override
  public void unlockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
    dataStore.unlockForWrite(db);
  }
}
