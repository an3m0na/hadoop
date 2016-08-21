package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.store.DataStoreImporter;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 7/26/16.
 */
public class HistorySnapshotMockDSImpl extends MockDataStoreImpl implements HistorySnapshotDS {

    private Long traceStart = 0L;
    private Long traceFinish = 0L;
    private Long currentOffset = 0L;
    private Long currentTime = 0L;
    private DataEntityDB mainDB = DataEntityDB.getMain();
    private DataEntityDB shadowDB = DataEntityDB.get(DataEntityDB.Type.MAIN, "shadow");

    public HistorySnapshotMockDSImpl(String dataDumpFolderName) {
        new DataStoreImporter(dataDumpFolderName).importTo(Utils.exposeDataStoreAsBroker(this));
        copy(mainDB, shadowDB);
        clear(mainDB);
    }

    private void recomputeSnapshot() {

        // gather ids of all apps, jobs and tasks that have finished in the new snapshot
        // delete them and their associated confs and counters from their active collections
        // bring all of their data from the official history collections to the shadow history collections
        // parse official history to delete these persisted entities and bring new entities that have started in the
            // new snapshot to the active collections in  shadow
    }

    private JobProfile obfuscateJobProfile(JobProfile job, List<TaskProfile> obfuscatedTasks) {
        job.setFinishTime(null);
        //TODO other counters and such
        return job;
    }

    @Override
    public Long getSnapshotTime() {
        return currentTime;
    }

    @Override
    public void setSnapshotTime(Long time) {
        currentTime = time;
        currentOffset = time - traceStart;
        recomputeSnapshot();
    }

    @Override
    public Long getSnapshotOffset() {
        return currentOffset;
    }

    @Override
    public void setSnapshotOffset(Long offset) {
        currentTime = traceStart + offset;
        currentOffset = offset;
        recomputeSnapshot();
    }

    @Override
    public Long getTraceStartTime() {
        return traceStart;
    }

    @Override
    public Long getTraceFinishTime() {
        return traceFinish;
    }
}
