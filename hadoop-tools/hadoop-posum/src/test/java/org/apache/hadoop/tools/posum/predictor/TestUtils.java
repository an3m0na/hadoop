package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by ane on 3/3/16.
 */
public class TestUtils {

    static Configuration getConf() {
        Configuration conf = new Configuration(false);
        conf.addResource("posum-core.xml");
        return conf;
    }
}
