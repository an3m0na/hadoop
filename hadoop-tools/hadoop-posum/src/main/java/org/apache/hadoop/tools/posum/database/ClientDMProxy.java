package org.apache.hadoop.tools.posum.database;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
 * Created by ane on 3/20/16.
 */
public class ClientDMProxy {

    private static final Log LOG = LogFactory.getLog(ClientDMProxy.class);

    protected static DataMasterProtocol createDMProxy(final Configuration configuration) throws IOException {
        YarnConfiguration conf = (configuration instanceof YarnConfiguration)
                ? (YarnConfiguration) configuration
                : new YarnConfiguration(configuration);
        RetryPolicy retryPolicy = createRetryPolicy(conf);

        InetSocketAddress dmAddress = getDMAddress(conf);
        LOG.info("Connecting to DataMaster at " + dmAddress);
        DataMasterProtocol proxy = getProxy(conf, dmAddress);
        return (DataMasterProtocol) RetryProxy.create(DataMasterProtocol.class, proxy, retryPolicy);
    }

    protected static DataMasterProtocol getProxy(final Configuration conf, final InetSocketAddress address) throws IOException {
        return UserGroupInformation.getCurrentUser().doAs(
                new PrivilegedAction<DataMasterProtocol>() {
                    @Override
                    public DataMasterProtocol run() {
                        return (DataMasterProtocol) YarnRPC.create(conf).getProxy(DataMasterProtocol.class, address, conf);
                    }
                });
    }

    protected static InetSocketAddress getDMAddress(YarnConfiguration conf) throws IOException {
        return conf.getSocketAddr(POSUMConfiguration.DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_PORT);
    }

    protected static RetryPolicy createRetryPolicy(Configuration conf) {
        long rmConnectWaitMS =
                conf.getLong(
                        POSUMConfiguration.POSUM_CONNECT_MAX_WAIT_MS,
                        POSUMConfiguration.DEFAULT_POSUM_CONNECT_MAX_WAIT_MS);
        long rmConnectionRetryIntervalMS =
                conf.getLong(
                        POSUMConfiguration.POSUM_CONNECT_RETRY_INTERVAL_MS,
                        POSUMConfiguration
                                .DEFAULT_POSUM_CONNECT_RETRY_INTERVAL_MS);

        boolean waitForEver = (rmConnectWaitMS == -1);
        if (!waitForEver) {
            if (rmConnectWaitMS < 0) {
                throw new YarnRuntimeException("Invalid Configuration. "
                        + YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS
                        + " can be -1, but can not be other negative numbers");
            }

            // try connect once
            if (rmConnectWaitMS < rmConnectionRetryIntervalMS) {
                LOG.warn(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS
                        + " is smaller than "
                        + YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS
                        + ". Only try connect once.");
                rmConnectWaitMS = 0;
            }
        }

        // Handle HA case first
        if (HAUtil.isHAEnabled(conf)) {
            final long failoverSleepBaseMs = conf.getLong(
                    YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS,
                    rmConnectionRetryIntervalMS);

            final long failoverSleepMaxMs = conf.getLong(
                    YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_MAX_MS,
                    rmConnectionRetryIntervalMS);

            int maxFailoverAttempts = conf.getInt(
                    YarnConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, -1);

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
                    YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS +
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
