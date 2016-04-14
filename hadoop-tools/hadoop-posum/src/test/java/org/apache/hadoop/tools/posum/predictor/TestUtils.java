package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;

/**
 * Created by ane on 3/3/16.
 */
public class TestUtils {

    static Configuration getConf() {
        return POSUMConfiguration.newInstance();
    }
}
