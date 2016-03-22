package org.apache.hadoop.tools.posum.database.store;

/**
 * Created by ane on 3/22/16.
 */
public interface DataTransaction {

    void run() throws Exception;
}
