# simworkr
The `simworkr` package provides a simulation framework for running distributed R computations. The worker nodes obtain computation jobs from a central Redis server. Results are then sent to the Redis database to be stored. Researchers can retrieve and aggregate completed simulation results at any moment in time.

The package was created to support the longitudinal cluster method evaluation as part of the [COVID-19 longitudinal cluster analysis case study](https://github.com/niekdt/meanvar-clustering-longitudinal-data). The code used in the package is derived from an earlier [simulation study](https://github.com/niekdt/comparison-clustering-longitudinal-data) on comparing methods for longitudinal clustering.

## Note
This package depends on the [rredis](https://github.com/bwlewis/rredis) package, which is no longer being developed, although still functional (for now). If there is sufficient interest, I will update the `simworkr` package to use the `redux` package instead of the defunct `rredis` package.
