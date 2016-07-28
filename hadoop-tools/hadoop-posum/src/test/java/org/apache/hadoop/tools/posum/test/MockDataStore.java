package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.tools.posum.database.client.DataClientInterface;

import java.io.IOException;

/**
 * Created by ane on 7/26/16.
 */
public interface MockDataStore extends DataClientInterface {
    void importData(String dataDumpPath) throws IOException;
    void exportData(String dataDumpPath) throws IOException;
}
