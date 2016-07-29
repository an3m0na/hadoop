package org.apache.hadoop.tools.posum.common.records.request;

import org.apache.hadoop.tools.posum.common.records.field.SimulationResult;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by ane on 4/20/16.
 */
public abstract class HandleSimResultRequest {

    public static HandleSimResultRequest newInstance() {
        return Records.newRecord(HandleSimResultRequest.class);
    }

    public static HandleSimResultRequest newInstance(List<SimulationResult> results) {
        HandleSimResultRequest request = newInstance();
        request.setResults(results);
        return request;
    }

    public abstract void setResults(List<SimulationResult> results);

    //the underlying implementation should be thread safe and ordered ascending by score
    public abstract ConcurrentSkipListSet<SimulationResult> getResults();

    public abstract void addResult(SimulationResult result);
}