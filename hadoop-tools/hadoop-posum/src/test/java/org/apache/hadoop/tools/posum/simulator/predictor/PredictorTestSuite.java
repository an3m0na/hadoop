package org.apache.hadoop.tools.posum.simulator.predictor;


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
