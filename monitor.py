'''
During load, measure, every 10 seconds
1. Average, 90th centile, 95th centile, 99th centile and max job loading and/or processing time
2. Total number of items processed every second by queue
3. Queue sizes
4. Memory size of queueing server
5. CPU utilization of queueing server

Usage:

    python monitor --graphite=host:port --prefix=

'''
import argparse


