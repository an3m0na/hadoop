package org.apache.hadoop.tools.posum.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.UserGroupInformation;
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
 * Created by ane on 3/21/16.
 */
public class StandardClientProxyFactory<T> {

    private static final Log LOG = LogFactory.getLog(StandardClientProxyFactory.class);

    private final Configuration conf;
    private final AcceptableProtocol<T> protocol;

    private static class AcceptableProtocol<P> {
        public Class<P> pClass;
        public String address;
        public String default_address;
        public int port;

        public AcceptableProtocol(Class<P> pClass, String address, String default_address, int port) {
            this.pClass = pClass;
            this.address = address;
            this.default_address = default_address;
            this.port = port;
        }
    }

    private static Map<Class, AcceptableProtocol> protocols;

    static {
        protocols = new HashMap<>();
        protocols.put(DataMasterProtocol.class, new AcceptableProtocol<>(
                DataMasterProtocol.class,
                POSUMConfiguration.DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_PORT
        ));
    }

    public StandardClientProxyFactory(Configuration conf, Class<T> protocol) {
        this.conf = conf;
        this.protocol = protocols.get(protocol);
        if (this.protocol == null) {
            throw new POSUMException("Protocol not acceptable " + protocol);
        }
    }

    public T createProxy() throws IOException {
        YarnConfiguration conf = (this.conf instanceof YarnConfiguration)
                ? (YarnConfiguration) this.conf
                : new YarnConfiguration(this.conf);
        RetryPolicy retryPolicy = createRetryPolicy(conf);

        InetSocketAddress remoteAddress = getRemoteAddress(conf);
        LOG.info("Connecting via" + protocol.pClass.getName() + " at " + remoteAddress);
        T proxy = getProxy(remoteAddress);
        return (T) RetryProxy.create(protocol.pClass, proxy, retryPolicy);
    }

    private T getProxy(final InetSocketAddress address) throws IOException {
        return UserGroupInformation.getCurrentUser().doAs(
                new PrivilegedAction<T>() {
                    @Override
                    public T run() {
                        return (T) YarnRPC.create(conf).getProxy(protocol.pClass, address, conf);
                    }
                });
    }

    protected InetSocketAddress getRemoteAddress(YarnConfiguration conf) throws IOException {
        return conf.getSocketAddr(protocol.address, protocol.default_address, protocol.port);
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
                        + POSUMConfiguration.POSUM_CONNECT_MAX_WAIT_MS
                        + " can be -1, but can not be other negative numbers");
            }

            // try connect once
            if (rmConnectWaitMS < rmConnectionRetryIntervalMS) {
                LOG.warn(POSUMConfiguration.POSUM_CONNECT_MAX_WAIT_MS
                        + " is smaller than "
                        + POSUMConfiguration.POSUM_CONNECT_RETRY_INTERVAL_MS
                        + ". Only try connect once.");
                rmConnectWaitMS = 0;
            }
        }

        // Handle HA case first
        if (HAUtil.isHAEnabled(conf)) {
            final long failoverSleepBaseMs = conf.getLong(
                    POSUMConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS,
                    rmConnectionRetryIntervalMS);

            final long failoverSleepMaxMs = conf.getLong(
                    POSUMConfiguration.CLIENT_FAILOVER_SLEEPTIME_MAX_MS,
                    rmConnectionRetryIntervalMS);

            int maxFailoverAttempts = conf.getInt(
                    POSUMConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, -1);

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
                    POSUMConfiguration.POSUM_CONNECT_RETRY_INTERVAL_MS +
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