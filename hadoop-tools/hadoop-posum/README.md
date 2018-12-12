# POSUM - A Portfolio Scheduler for MapReduce

This module contains the source code for POSUM, a portfolio scheduler for MapReduce applications. 
This system runs as a set of processes external to the Hadoop cluster and allows for dynamically switching Hadoop's active scheduling policy to achieve (compound) performance objectives under varying workload characteristics.
It does this by periodically evaluating a set of potential scheduling policies and scoring them according to an evaluation function that can span different metrics.
The tool also provides a graphical interface for monitoring its operation and online administrator tuning.

For more information, refer to [this thesis document](http://resolver.tudelft.nl/uuid:ba70bc56-956e-4b8e-b532-d68842c1c830).

For helper scripts for running on the [DAS5 datacenter](https://www.cs.vu.nl/das5/), check out the [posum-tools repo](https://atlarge.ewi.tudelft.nl/gitlab/m.voinea/posum-tools).
