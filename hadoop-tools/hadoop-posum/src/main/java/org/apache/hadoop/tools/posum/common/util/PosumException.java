package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

public class PosumException extends YarnRuntimeException {

    public PosumException(Throwable cause) {
        super(cause);
    }

    public PosumException(String message) {
        super(message);
    }

    public PosumException(String message, Throwable cause) {
        super(message, cause);
    }
}
