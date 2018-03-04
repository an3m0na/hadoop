package org.apache.hadoop.tools.posum.simulation.predictor.stats;

import com.sun.jersey.core.util.Base64;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class RegressionStatEntry extends SimpleRegression implements PredictionStatEntry<RegressionStatEntry> {
  private Double simpleSlope = null;

  @Override
  public long getSampleSize() {
    return getN();
  }

  @Override
  public RegressionStatEntry copy() {
    return new RegressionStatEntry().merge(this);
  }

  @Override
  public RegressionStatEntry merge(RegressionStatEntry otherEntry) {
    if (this.simpleSlope == null)
      this.simpleSlope = otherEntry.simpleSlope;
    append(otherEntry);
    return this;
  }

  @Override
  public String serialize() {
    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
         ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
      objectStream.writeObject(this);
      return new String(Base64.encode(byteStream.toByteArray()));
    } catch (IOException e) {
      throw new PosumException("Could not serialize regression stat entry", e);
    }
  }

  @Override
  public RegressionStatEntry deserialize(String serializedForm) {
    if (serializedForm == null)
      return null;
    byte[] decodedBytes = Base64.decode(serializedForm);
    try (ByteArrayInputStream byteStream = new ByteArrayInputStream(decodedBytes);
         ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
      RegressionStatEntry deserializedRegression = (RegressionStatEntry) objectStream.readObject();
      append(deserializedRegression);
    } catch (IOException | ClassNotFoundException e) {
      throw new PosumException("Could not deserialize regression stat entry", e);
    }
    return this;
  }
}
