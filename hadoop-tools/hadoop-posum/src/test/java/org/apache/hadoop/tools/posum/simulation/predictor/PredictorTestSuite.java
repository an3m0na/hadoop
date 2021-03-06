package org.apache.hadoop.tools.posum.simulation.predictor;


import org.apache.hadoop.tools.posum.simulation.predictor.basic.TestBasicPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.detailed.TestDetailedPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.standard.TestStandardPredictor;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  TestBasicPredictor.class,
  TestStandardPredictor.class,
  TestDetailedPredictor.class
})

public class PredictorTestSuite {

}
