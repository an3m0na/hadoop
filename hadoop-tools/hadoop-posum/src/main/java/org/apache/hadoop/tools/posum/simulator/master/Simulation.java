package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

/**
 * Created by ane on 4/20/16.
 */
public class Simulation extends Thread{
    private boolean exit = false;

    public Simulation(String policy, JobBehaviorPredictor predictor){
        //TODO set up simulation
    }

    void exit() {
        exit = true;
        interrupt();
    }

    @Override
    public void run() {
        while (!exit) {
            //TODO
        }
    }
}
