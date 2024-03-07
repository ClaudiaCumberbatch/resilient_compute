# resilient_compute

Resilient compute pool: This solution will allow for the construction of resource pools that work to maintain, in a resilient fashion, sets of resources with specified properties: e.g., minimum total size, a certain geographical distribution, minimum number of nodes per site. 

Intended uses include multi-site applications (e.g., federated learning) that require a minimum resource level at each site, with resource deficits due to increased demand or reduced supply potentially compromising overall progress or leading to subtle errors such as bias due to reduced contributions from straggling training tasks. 

We will build upon funcX82, a federated FaaS platform that allows users to provision compute pools on different resources (HPC, Cloud, edge) and for users to then dispatch computational tasks to remote compute pools. We will extend this model to consume event streams, potentially use models to predict imbalances, and use the policy engine to drive adaptive healing actions, such as dynamically reconfiguring workload partitions (e.g., assign less work to stragglers) and/or adding/replacing resources in an elastic fashion (e.g., new HPC reservation).