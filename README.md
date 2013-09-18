# Queueing system performance test

The goal is to write some quick python script to performance test queuing systems.

Ideally, we'd like to try to figure out the following things:
- Maximum throughput of a system on  hardware
- How much memory per job is required

## Test is in four steps

- Load X jobs with N workers
- Process X jobs over S steps from a previously loaded system
- Load and process X jobs over S steps simultaneously with N workers + N/S loaders
    - Start with Y jobs already in system
    - Start with an empty system

## Measurement

As a first step, we only measure how long it took to process the X messages, overall.

Ideally, during load, measure, every 5 seconds
- Average, 90th centile, 95th centile, 99th centile and max job loading and/or processing time
- Total number of items processed every second by queue
- Queue sizes
- Memory size of queueing server
- CPU utilization of queueing server

Also, optionally, put in a log file the job ID and processing times. Then make
sure all job were processed through, and compare job latency.

## Process definition

Loader create job in queue0. Size of job payload should be configurable.

Worker pick job from queue n, and put it in q n+1, unless n == S, then they mark job as complete

## Preliminary target list

- qless
- beanstalkd
- ...

