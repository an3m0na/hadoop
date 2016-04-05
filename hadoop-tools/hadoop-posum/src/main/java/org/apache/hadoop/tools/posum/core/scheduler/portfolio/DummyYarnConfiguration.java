package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Map;

/**
 * Created by ane on 4/5/16.
 */
public class DummyYarnConfiguration extends YarnConfiguration {
    public DummyYarnConfiguration(Map<String, String> properties) {
        super();
        for (Map.Entry<String, String> property : properties.entrySet()) {
            set(property.getKey(), property.getValue());
        }
    }
}
