package org.apache.hadoop.tools.posum.simulation.predictor.stats;

import com.sun.jersey.core.util.Base64;
import org.apache.commons.math3.stat.regression.ModelSpecificationException;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class RegressionStatEntry extends SimpleRegression implements PredictionStatEntry<RegressionStatEntry> {
  private Double simpleRate = null;

  public Double getPrediction(double inputSize) {
    if (getN() < 2)
      return simpleRate == null ? null : inputSize / simpleRate;
    return predict(inputSize);
  }

  @Override
  public void addData(double x, double y) {
    super.addData(x, y);
    if (getN() == 1) {
      if (y == 0)
        throw new PosumException("Invalid duration 0 for regression stat entry");
      simpleRate = Math.max(x, 1.0) / y; // restrict to at least 1 input byte to avoid division by 0
    }
  }

  @Override
  public void addData(double[][] data) throws ModelSpecificationException {
    if (data.length == 1 && data[0].length >= 2) {
      addData(data[0][0], data[0][1]);
      return;
    }
    super.addData(data);

  }

  @Override
  public void addObservation(double[] x, double y) throws ModelSpecificationException {
    if (x.length == 1) {
      addData(x[0], y);
      return;
    }
    super.addObservation(x, y);
  }

  @Override
  public void addObservations(double[][] x, double[] y) throws ModelSpecificationException {
    super.addObservations(x, y);
  }

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
