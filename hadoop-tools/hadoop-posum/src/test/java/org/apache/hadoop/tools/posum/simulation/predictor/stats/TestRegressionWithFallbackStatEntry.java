package org.apache.hadoop.tools.posum.simulation.predictor.stats;

import org.junit.Test;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestRegressionWithFallbackStatEntry {
  @Test
  public void testMerge() {
    RegressionWithFallbackStatEntry entry = new RegressionWithFallbackStatEntry();
    entry.addSample(1.0, 3.5);
    entry.addSample(2.0, 2.788);
    assertThat(entry.getSampleSize(), is(2L));
    assertThat(entry.predict(1.0), closeTo(3.5, 0.00000001));
    assertThat(entry.predict(2.0), closeTo(2.788, 0.00000001));
    assertThat(entry.predict(3.0), is(2.076));
    assertThat(entry.getAverage(), is(3.144));

    RegressionWithFallbackStatEntry otherEntry = new RegressionWithFallbackStatEntry();
    otherEntry.addSample(4.267, 1.75);
    otherEntry.addSample(7.22, 5.88);
    otherEntry.addSample(23456.0, 8532.53);
    assertThat(otherEntry.getSampleSize(), is(3L));
    assertThat(otherEntry.predict(1.0), is(2.089721768079921));
    assertThat(otherEntry.getAverage(), is(2846.72));

    entry.merge(otherEntry);
    assertThat(entry.getSampleSize(), is(5L));
    assertThat(entry.predict(1.0), is(2.5260313920516313));
    assertThat(entry.getAverage(), is(1709.2896));
  }

  @Test
  public void testSerialization() {
    RegressionWithFallbackStatEntry entry = new RegressionWithFallbackStatEntry();
    entry.addSample(4.0, 5.0);
    entry.addSample(2.0, 1.0);
    String serialized = entry.serialize();
    RegressionWithFallbackStatEntry newEntry = new RegressionWithFallbackStatEntry().deserialize(serialized);

    assertThat(newEntry.getSampleSize(), is(entry.getSampleSize()));
    assertThat(newEntry.getRegression().getIntercept(), is(entry.getRegression().getIntercept()));
    assertThat(newEntry.getRegression().getSlope(), is(entry.getRegression().getSlope()));
    assertThat(newEntry.predict(7.0), is(entry.predict(7.0)));
  }

  @Test
  public void testInsufficientData() {
    RegressionWithFallbackStatEntry entry = new RegressionWithFallbackStatEntry();
    assertThat(entry.predict(0), nullValue());
    assertThat(entry.predict(4.0), nullValue());
    assertThat(entry.getAverage(), nullValue());
    entry.addSample(4.0, 5.0);
    assertThat(entry.predict(4.0), is(5.0));
    assertThat(entry.predict(2.0), is(2.5));
    assertThat(entry.predict(0.0), is(0.0));
    assertThat(entry.getAverage(), is(5.0));
    entry.addSample(2.0, 5.0);
    assertThat(entry.predict(1.0), is(5.0));
    assertThat(entry.getAverage(), is(5.0));
  }
}
