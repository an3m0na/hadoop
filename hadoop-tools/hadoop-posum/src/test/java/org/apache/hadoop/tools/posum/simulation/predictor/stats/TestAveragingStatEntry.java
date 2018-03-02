package org.apache.hadoop.tools.posum.simulation.predictor.stats;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestAveragingStatEntry {
  @Test
  public void testMerge() {
    AveragingStatEntry entry = new AveragingStatEntry();
    entry.addSample(3.5);
    entry.addSample(2.788);
    assertThat(entry.getSampleSize(), is(2L));
    assertThat(entry.getAverage(), is(3.144));

    AveragingStatEntry otherEntry = new AveragingStatEntry();
    otherEntry.addSample(1.75);
    otherEntry.addSample(5.88);
    otherEntry.addSample(8532.53);
    assertThat(otherEntry.getSampleSize(), is(3L));
    assertThat(otherEntry.getAverage(), is(2846.72));

    entry.merge(otherEntry);
    assertThat(entry.getSampleSize(), is(5L));
    assertThat(entry.getAverage(), is(1709.2896));
  }

  @Test
  public void testSerialization() {
    AveragingStatEntry entry = new AveragingStatEntry();
    entry.addSample(3.556789);
    String serialized = entry.serialize();
    assertThat(serialized, is("1=3.556789"));

    AveragingStatEntry newEntry = new AveragingStatEntry().deserialize(serialized);
    assertThat(newEntry.getSampleSize(), is(entry.getSampleSize()));
    assertThat(newEntry.getAverage(), is(entry.getAverage()));
  }
}
