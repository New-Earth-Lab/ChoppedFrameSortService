#!/bin/bash
# Start with current project activated, two general threads, and one thread in the interactive threadpool
# Default arugment value: /opt/spiders/choppedframesortservice/config.toml
julia +1.10 --project=/opt/spiders/choppedframesortservice/ --threads 1,1 --gcthreads=1 -e "using ChoppedFrameSortService; ChoppedFrameSortService.main(ARGS)" -- ${1:-/opt/spiders/choppedframesortservice/config.toml}
