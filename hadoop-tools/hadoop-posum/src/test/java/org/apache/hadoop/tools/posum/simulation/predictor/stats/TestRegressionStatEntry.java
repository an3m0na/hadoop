package org.apache.hadoop.tools.posum.simulation.predictor.stats;

import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.junit.Test;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestRegressionStatEntry {
  @Test
  public void testMerge() {
    RegressionStatEntry entry = new RegressionStatEntry();
    entry.addData(1.0, 3.5);
    entry.addData(2.0, 2.788);
    assertThat(entry.getSampleSize(), is(2L));
    assertThat(entry.getPrediction(1.0), closeTo(3.5, 0.00000001));
    assertThat(entry.getPrediction(2.0), closeTo(2.788, 0.00000001));
    assertThat(entry.getPrediction(3.0), is(2.076));

    RegressionStatEntry otherEntry = new RegressionStatEntry();
    otherEntry.addData(4.267, 1.75);
    otherEntry.addData(7.22, 5.88);
    otherEntry.addData(23456.0, 8532.53);
    assertThat(otherEntry.getSampleSize(), is(3L));
    assertThat(otherEntry.getPrediction(1.0), is(2.089721768079921));

    entry.merge(otherEntry);
    assertThat(entry.getSampleSize(), is(5L));
    assertThat(entry.getPrediction(1.0), is(2.5260313920516313));
  }

  @Test
  public void testSerialization() {
    RegressionStatEntry entry = new RegressionStatEntry();
    entry.addData(4.0, 5.0);
    entry.addData(2.0, 1.0);
    String serialized = entry.serialize();
    RegressionStatEntry newEntry = new RegressionStatEntry().deserialize(serialized);

    assertThat(newEntry.getSampleSize(), is(entry.getSampleSize()));
    assertThat(newEntry.getIntercept(), is(entry.getIntercept()));
    assertThat(newEntry.getSlope(), is(entry.getSlope()));
    assertThat(newEntry.predict(7.0), is(entry.predict(7.0)));
  }

  @Test
  public void testInsufficientData() {
    RegressionStatEntry entry = new RegressionStatEntry();
    assertThat(entry.getPrediction(0), nullValue());
    assertThat(entry.getPrediction(4.0), nullValue());
    entry.addData(4.0, 5.0);
    assertThat(entry.getPrediction(4.0), is(5.0));
    assertThat(entry.getPrediction(2.0), is(2.5));
    assertThat(entry.getPrediction(0.0), is(0.0));
    entry.addData(2.0, 5.0);
    assertThat(entry.getPrediction(1.0), is(5.0));
  }

  @Test
  public void testInputBound() {
    RegressionStatEntry entry = new RegressionStatEntry();
    entry.addData(0.0, 5.0);

    assertThat(entry.getPrediction(1.0), is(5.0));
    assertThat(entry.getPrediction(100.0), is(500.0));
  }

  @Test(expected = PosumException.class)
  public void testInvalidData() {
    new RegressionStatEntry().addData(1.0, 0.0);
  }
}
