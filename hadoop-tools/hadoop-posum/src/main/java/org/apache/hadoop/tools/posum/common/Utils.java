package org.apache.hadoop.tools.posum.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

/**
 * Created by ane on 3/3/16.
 */
public class Utils {

    private static Log logger = LogFactory.getLog(Utils.class);

    public static TaskType getTaskTypeFromId(String id) {
        try {
            String[] parts = id.split("_");
            return "m".equals(parts[parts.length - 2]) ? TaskType.MAP : TaskType.REDUCE;
        } catch (Exception e) {
            logger.error("[Utils] Could not get task type from id " + id, e);
        }
        return null;
    }
}
