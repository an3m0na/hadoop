package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.tools.posum.simulation.predictor.basic.BasicPredictor;
import org.junit.Test;

public class TestBasicPredictor extends TestPredictor<BasicPredictor> {
  public TestBasicPredictor() {
    super(BasicPredictor.class);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testGetComparableProfiles() throws Exception {


  }
}
