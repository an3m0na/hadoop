package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ane on 3/19/16.
 */
public class DummyTokenIdentifier extends TokenIdentifier {
    public static final Text KIND_NAME = new Text("POSUM_DUMMY_TOKEN");

    private final String id = "dummy";

    public String getId() {
        return id;
    }

    @Override
    public Text getKind() {
        return KIND_NAME;
    }

    @Override
    public UserGroupInformation getUser() {
        return UserGroupInformation.createRemoteUser(id);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(id.getBytes());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte[] buffer = new byte[128];
        in.readFully(buffer);
    }

    @Override
    public int hashCode() {
        return getProto().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other.getClass().isAssignableFrom(this.getClass())) {
            return this.getProto().equals(this.getClass().cast(other).getProto());
        }
        return false;
    }

    public String getProto() {
        //we dont have a proto form
        return this.id;
    }
}
