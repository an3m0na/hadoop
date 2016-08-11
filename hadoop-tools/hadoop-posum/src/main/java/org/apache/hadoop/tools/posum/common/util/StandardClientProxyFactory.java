package org.apache.hadoop.tools.posum.common.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.EOFException;
import java.io.IOException;
import java.net.*;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by ane on 3/21/16.
 */
public class StandardClientProxyFactory<T> {

    private static final Log LOG = LogFactory.getLog(StandardClientProxyFactory.class);

    private final Configuration conf;
    private final Class<T> protocol;
    private final String address;
    private final String defaultAddress;
    private final int port;


    public StandardClientProxyFactory(Configuration conf,
                                      String address,
                                      String defaultAddress,
                                      int port,
                                      Class<T> protocol) {
        this.conf = conf;
        this.address = address;
        this.defaultAddress = defaultAddress;
        this.port = port;
        this.protocol = protocol;
    }

    public T createProxy() throws IOException {
        RetryPolicy retryPolicy = createRetryPolicy(conf);

        String target = address != null ? address : defaultAddress;
        InetSocketAddress remoteAddress = NetUtils.createSocketAddr(target, port);
        LOG.info("Connecting via " + protocol.getName() + " at " + remoteAddress);
        T proxy = getProxy(remoteAddress);
        return (T) RetryProxy.create(protocol, proxy, retryPolicy);
    }

    private T getProxy(final InetSocketAddress address) throws IOException {
        return UserGroupInformation.getCurrentUser().doAs(
                new PrivilegedAction<T>() {
                    @Override
                    public T run() {
                        return (T) YarnRPC.create(conf).getProxy(protocol, address, conf);
                    }
                });
    }

    protected static RetryPolicy createRetryPolicy(Configuration conf) {
        long rmConnectWaitMS =
                conf.getLong(
                        PosumConfiguration.POSUM_CONNECT_MAX_WAIT_MS,
                        PosumConfiguration.POSUM_CONNECT_MAX_WAIT_MS_DEFAULT);
        long rmConnectionRetryIntervalMS =
                conf.getLong(
                        PosumConfiguration.POSUM_CONNECT_RETRY_INTERVAL_MS,
                        PosumConfiguration
                                .POSUM_CONNECT_RETRY_INTERVAL_MS_DEFAULT);

        boolean waitForEver = (rmConnectWaitMS == -1);
        if (!waitForEver) {
            if (rmConnectWaitMS < 0) {
                throw new YarnRuntimeException("Invalid Configuration. "
                        + PosumConfiguration.POSUM_CONNECT_MAX_WAIT_MS
                        + " can be -1, but can not be other negative numbers");
            }

            // try connect once
            if (rmConnectWaitMS < rmConnectionRetryIntervalMS) {
                LOG.warn(PosumConfiguration.POSUM_CONNECT_MAX_WAIT_MS
                        + " is smaller than "
                        + PosumConfiguration.POSUM_CONNECT_RETRY_INTERVAL_MS
                        + ". Only try connect once.");
                rmConnectWaitMS = 0;
            }
        }

        // Handle HA case first
        if (HAUtil.isHAEnabled(conf)) {
            final long failoverSleepBaseMs = conf.getLong(
                    PosumConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS,
                    rmConnectionRetryIntervalMS);

            final long failoverSleepMaxMs = conf.getLong(
                    PosumConfiguration.CLIENT_FAILOVER_SLEEPTIME_MAX_MS,
                    rmConnectionRetryIntervalMS);

            int maxFailoverAttempts = conf.getInt(
                    PosumConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, -1);

            if (maxFailoverAttempts == -1) {
                if (waitForEver) {
                    maxFailoverAttempts = Integer.MAX_VALUE;
                } else {
                    maxFailoverAttempts = (int) (rmConnectWaitMS / failoverSleepBaseMs);
                }
            }

            return RetryPolicies.failoverOnNetworkException(
                    RetryPolicies.TRY_ONCE_THEN_FAIL, maxFailoverAttempts,
                    failoverSleepBaseMs, failoverSleepMaxMs);
        }

        if (rmConnectionRetryIntervalMS < 0) {
            throw new YarnRuntimeException("Invalid Configuration. " +
                    PosumConfiguration.POSUM_CONNECT_RETRY_INTERVAL_MS +
                    " should not be negative.");
        }

        RetryPolicy retryPolicy = null;
        if (waitForEver) {
            retryPolicy = RetryPolicies.RETRY_FOREVER;
        } else {
            retryPolicy =
                    RetryPolicies.retryUpToMaximumTimeWithFixedSleep(rmConnectWaitMS,
                            rmConnectionRetryIntervalMS, TimeUnit.MILLISECONDS);
        }

        Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
                new HashMap<Class<? extends Exception>, RetryPolicy>();

        exceptionToPolicyMap.put(EOFException.class, retryPolicy);
        exceptionToPolicyMap.put(ConnectException.class, retryPolicy);
        exceptionToPolicyMap.put(NoRouteToHostException.class, retryPolicy);
        exceptionToPolicyMap.put(UnknownHostException.class, retryPolicy);
        exceptionToPolicyMap.put(ConnectTimeoutException.class, retryPolicy);
        exceptionToPolicyMap.put(RetriableException.class, retryPolicy);
        exceptionToPolicyMap.put(SocketException.class, retryPolicy);

        return RetryPolicies.retryByException(
                RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    }
}