package org.apache.hadoop.tools.posum.scheduler.portfolio;


import org.apache.hadoop.tools.posum.scheduler.portfolio.edls.TestEDLSPriority;
import org.apache.hadoop.tools.posum.scheduler.portfolio.edls.TestEDLSShare;
import org.apache.hadoop.tools.posum.scheduler.portfolio.srtf.TestShortestRTFirstPolicy;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  TestEDLSShare.class,
  TestEDLSPriority.class,
  TestLocalityFirst.class,
  TestShortestRTFirstPolicy.class,
})

public class PortfolioTestSuite {

}
