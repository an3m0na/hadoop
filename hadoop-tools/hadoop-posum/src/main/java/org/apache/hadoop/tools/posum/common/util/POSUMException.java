package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * Created by ane on 3/21/16.
 */
public class POSUMException extends YarnRuntimeException {

    public POSUMException(Throwable cause) {
        super(cause);
    }

    public POSUMException(String message) {
        super(message);
    }

    public POSUMException(String message, Throwable cause) {
        super(message, cause);
    }
}
