package org.apache.hadoop.tools.posum.scheduler.portfolio.srtf;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.DatabaseProvider;
import org.apache.hadoop.tools.posum.data.mock.data.MockDataStoreImpl;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.tools.posum.common.util.Utils.parseApplicationId;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAppWorkCalculator {
  private Database db;
  private AppWorkCalculator subject;
  private SRTFAppAttempt mockApp;

  @Before
  public void setUp() throws Exception {
    db = Database.from(new MockDataStoreImpl(), DatabaseReference.getMain());
    subject = new AppWorkCalculator(new DatabaseProvider() {
      @Override
      public Database getDatabase() {
        return db;
      }
    });
    mockApp = mock(SRTFAppAttempt.class);
  }

  @Test
  public void testNoInfo(){
    AppProfile app = Utils.APP1.copy();
    JobProfile job = Utils.JOB1.copy();
    job.setTotalMapTasks(3);
    job.setTotalReduceTasks(2);

    db.execute(StoreCall.newInstance(DataEntityCollection.APP, app));
    db.execute(StoreCall.newInstance(DataEntityCollection.JOB, job));

    when(mockApp.getApplicationId()).thenReturn(parseApplicationId(app.getId()));

    subject.updateRemainingTime(mockApp);

    verify(mockApp).setJobId(job.getId());
    verify(mockApp).setSubmitTime(app.getStartTime());
    verify(mockApp, never()).setRemainingWork(anyLong());
  }

  @Test
  public void testMapOnly(){
    AppProfile app = Utils.APP1.copy();
    JobProfile job = Utils.JOB1.copy();
    job.setTotalMapTasks(3);
    job.setTotalReduceTasks(2);
    job.setCompletedMaps(1);
    job.setAvgMapDuration(200L);

    db.execute(StoreCall.newInstance(DataEntityCollection.APP, app));
    db.execute(StoreCall.newInstance(DataEntityCollection.JOB, job));

    when(mockApp.getApplicationId()).thenReturn(parseApplicationId(app.getId()));

    subject.updateRemainingTime(mockApp);

    verify(mockApp).setTotalWork(600L);
    verify(mockApp).setRemainingWork(400L);
  }

  @Test
  public void testReduceEstimation(){
    AppProfile app = Utils.APP1.copy();
    JobProfile job = Utils.JOB1.copy();
    job.setTotalMapTasks(3);
    job.setTotalReduceTasks(2);
    job.setCompletedMaps(1);
    job.setAvgMapDuration(200L);
    job.setTotalSplitSize(1000000L);
    job.setMapOutputBytes(500000L);

    db.execute(StoreCall.newInstance(DataEntityCollection.APP, app));
    db.execute(StoreCall.newInstance(DataEntityCollection.JOB, job));

    when(mockApp.getApplicationId()).thenReturn(parseApplicationId(app.getId()));

    subject.updateRemainingTime(mockApp);

    verify(mockApp).setTotalWork(900L);
    verify(mockApp).setRemainingWork(700L);
  }

  @Test
  public void testFull(){
    AppProfile app = Utils.APP1.copy();
    JobProfile job = Utils.JOB1.copy();
    job.setTotalMapTasks(3);
    job.setTotalReduceTasks(2);
    job.setCompletedMaps(1);
    job.setAvgMapDuration(200L);
    job.setCompletedReduces(1);
    job.setAvgReduceDuration(1000L);

    db.execute(StoreCall.newInstance(DataEntityCollection.APP, app));
    db.execute(StoreCall.newInstance(DataEntityCollection.JOB, job));

    when(mockApp.getApplicationId()).thenReturn(parseApplicationId(app.getId()));

    subject.updateRemainingTime(mockApp);

    verify(mockApp).setTotalWork(2600L);
    verify(mockApp).setRemainingWork(1400L);
  }
}
