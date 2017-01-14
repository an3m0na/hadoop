package org.apache.hadoop.tools.posum.simulation.core.daemons;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Private
@Unstable
public class DaemonUtils {

    public static String[] getRackHostName(String hostname) {
        hostname = hostname.substring(1);
        return hostname.split("/");
    }
}
