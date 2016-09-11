package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.junit.*;
import org.mockito.*;

import java.lang.Long;
import java.net.URL;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.Utils.ID_FIELD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.*;


public class TestHistorySnapshotBrokerImpl {
    private HistorySnapshotBrokerImpl testSubject;


    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        URL workloadUrl = getClass().getClassLoader().getResource("test_workload");
        if (workloadUrl == null)
            throw new RuntimeException("Default test workload folder was not found");
        testSubject = new HistorySnapshotBrokerImpl(workloadUrl.getPath());
    }

    @Test
    public void testTimeManipulation() {
        Long start = 1470865804940L, finish = 1470867152132L;
        assertEquals(start, testSubject.getTraceStartTime());
        assertEquals(finish, testSubject.getTraceFinishTime());
        Long now = System.currentTimeMillis();
        testSubject.setSnapshotTime(now);
        assertEquals(now, testSubject.getSnapshotTime());
        Long offset = now - start;
        assertEquals(offset, testSubject.getSnapshotOffset());
        testSubject.setSnapshotOffset(0L);
        assertEquals(Long.valueOf(0), testSubject.getSnapshotOffset());
        assertEquals(start, testSubject.getSnapshotTime());
    }

    @Test
    public void testDefaultSnapshot() {
        Long defaultSnapshotTime = 1470866275000L;
        testSubject.setSnapshotTime(defaultSnapshotTime);
        IdsByQueryCall getIds = IdsByQueryCall.newInstance(JOB_HISTORY, null, ID_FIELD, false);

        // check that finished jobs are there
        List<String> jobIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(6, jobIds.size());

        // check that their confs and counters are there
        getIds.setEntityCollection(JOB_CONF_HISTORY);
        List<String> associatedIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(6, associatedIds.size());
        getIds.setEntityCollection(COUNTER_HISTORY);
        getIds.setQuery(QueryUtils.in(ID_FIELD, jobIds));
        associatedIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(6, associatedIds.size());

        //check that their tasks are there
        getIds.setEntityCollection(TASK_HISTORY);
        getIds.setQuery(QueryUtils.in("jobId", jobIds));
        List<String> taskIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(251, taskIds.size());

        //check that their tasks' counters are there
        getIds.setEntityCollection(COUNTER_HISTORY);
        getIds.setQuery(QueryUtils.in(ID_FIELD, taskIds));
        associatedIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(251, associatedIds.size());

        // check that unfinished jobs are there
        getIds = IdsByQueryCall.newInstance(JOB, null, ID_FIELD, false);
        jobIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(2, jobIds.size());
        assertEquals("job_1470865342479_0007", jobIds.get(0));
        assertEquals("job_1470865342479_0008", jobIds.get(1));
        List<JobProfile> runningJobs = testSubject.executeDatabaseCall(
                FindByQueryCall.newInstance(JOB, QueryUtils.in(ID_FIELD, jobIds)), DataEntityDB.getMain()
        ).getEntities();
        for (JobProfile runningJob : runningJobs) {
            assertEquals(Long.valueOf(0L), runningJob.getFinishTime());
        }

        // check that their confs and counters are there
        getIds.setEntityCollection(JOB_CONF);
        getIds.setQuery(QueryUtils.in(ID_FIELD, jobIds));
        associatedIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(2, associatedIds.size());
        getIds.setEntityCollection(COUNTER);
        getIds.setQuery(QueryUtils.in(ID_FIELD, jobIds));
        associatedIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(0, associatedIds.size());

        //check that their tasks are there
        getIds.setEntityCollection(TASK);
        getIds.setQuery(QueryUtils.in("jobId", jobIds));
        taskIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(136, taskIds.size());

        //check that their finished tasks' counters are there
        getIds.setEntityCollection(COUNTER);
        getIds.setQuery(QueryUtils.in(ID_FIELD, taskIds));
        associatedIds = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        IdsByQueryCall getFinishedTaskIds = IdsByQueryCall.newInstance(TASK,
                QueryUtils.and(QueryUtils.in("jobId", jobIds), QueryUtils.isNot("finishTime", 0L)), ID_FIELD, false);
        List<String> finishedTaskIds =
                testSubject.executeDatabaseCall(getFinishedTaskIds, DataEntityDB.getMain()).getEntries();
        assertArrayEquals(finishedTaskIds.toArray(), associatedIds.toArray());

    }

    @Test
    public void testSecondSnapshot() {
        Long firstStep = 1470866065841L;
        // take a first ignored snapshot to see if it influences the default
        testSubject.setSnapshotTime(firstStep);
        testDefaultSnapshot();
    }

    @Test
    public void testEndSnapshot() {
        Long now = System.currentTimeMillis();
        testSubject.setSnapshotTime(now);
        IdsByQueryCall getIds = IdsByQueryCall.newInstance(JOB, null, ID_FIELD, true);
        List<String> ids = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(16, ids.size());
        getIds.setQuery(QueryUtils.is("finishTime", 0L));
        ids = testSubject.executeDatabaseCall(getIds, DataEntityDB.getMain()).getEntries();
        assertEquals(0, ids.size());
    }
} 
