# Welcome to Earthmover!

This is a small starter project that you can use to get up and running with [Earthmover](https://github.com/edanalytics/earthmover)

If your current working directory contains this README file and earthmover.yaml, you can run your Earthmover job by entering
```sh
earthmover run
```

The job described in earthmover.yaml takes two source files (found in the sources directory), applies some cleaning, and joins their rows via a union operation. It then writes the unioned dataset to outputs/animals.jsonl, a JSON lines file. Once your code is running, you can start adding your own sources, transformations, and destinations to get your own Earthmover project up and running!