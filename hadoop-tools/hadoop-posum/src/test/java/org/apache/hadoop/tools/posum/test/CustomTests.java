package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by ane on 4/20/16.
 */
public class CustomTests {
    @Test
    public void checkRegistration() {
        System.out.println(NetUtils.createSocketAddr("0.0.0.0", 7000));
    }

    @Test
    public void testMongoRunner() throws IOException, InterruptedException {
        String scriptLocation = getClass().getClassLoader().getResource("run-mongo.sh").getFile();
        Process process = Runtime.getRuntime().exec("/bin/bash " + scriptLocation);
        process.waitFor();
        if (process.exitValue() != 0) {
            System.out.println("Error running Mongo database:");
            String s;
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((s = reader.readLine()) != null)
                System.out.println(s);
            reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((s = reader.readLine()) != null)
                System.out.println(s);
            throw new RuntimeException("Could not run MongoDB");
        }
    }

    @Test
    public void testMongoStopper() throws IOException, InterruptedException {
        String scriptLocation = getClass().getClassLoader().getResource("run-mongo.sh").getFile();
        Process process = Runtime.getRuntime().exec("/bin/bash " + scriptLocation + " --stop");
        process.waitFor();
        if (process.exitValue() != 0) {
            System.out.println("Error stopping Mongo database:");
            String s;
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((s = reader.readLine()) != null)
                System.out.println(s);
            reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((s = reader.readLine()) != null)
                System.out.println(s);
            throw new RuntimeException("Could not stop MongoDB");
        }
    }
}
