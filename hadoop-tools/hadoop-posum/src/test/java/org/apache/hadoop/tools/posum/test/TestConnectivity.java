package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

/**
 * Created by ane on 4/20/16.
 */
public class TestConnectivity {
    @Test
    public void checkRegistration(){
        System.out.println(NetUtils.createSocketAddr("0.0.0.0", 7000));
    }
}
