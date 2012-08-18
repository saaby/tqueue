#!/usr/bin/python

# Generic threaded command queuer.
# Anders Kuhn Saaby
# anders@saaby.com
#
# Simple usage example:
# $ cat list_to_process | tqueue.py processor_script.sh

from time import sleep
from datetime import datetime
from sys import stdin, stdout, stderr, exit
from random import randint
from os import setpgrp
import subprocess
import threading
import Queue
import argparse
import logging

# Parse command arguments
parser = argparse.ArgumentParser(description="Threaded command queuer - input is read from stdin")
parser.add_argument("command", type=str,
        help="Command to execute - processing input lines as arguments")
parser.add_argument("-t", "--threads", type=int, default=4,
        help="Number of worker threads (default: 4)")
parser.add_argument("-q", "--queue-length", type=int, default=0,
        help="Work queue length (default: THREADS x 10)")
parser.add_argument("-l", "--loglevel", type=str, default="WARNING",
        help="Log verbosity: CRITICAL, WARNING, INFO (default: WARNING)")
parser.add_argument("-o", "--output-file", type=str, default="",
        help="Output file (default: stderr)")
parser.add_argument("-a", "--abort-sub-processes", action="store_true",
        help="Abort sub processes if CTRL-C is pressed (default: Sub processes are not aborted if CTRL-C is pressed)")

cmd_line_args = parser.parse_args()

#### Setup variables from cmd args ####
num_worker_threads = cmd_line_args.threads # Number of workers
command = cmd_line_args.command # processor command
abort_sub_processes = cmd_line_args.abort_sub_processes # Should sub processes be aborted?

# Sanitize -input and set logging parameters
logger_filename = cmd_line_args.output_file

logger = logging.getLogger(__name__)
numeric_log_level = getattr(logging, cmd_line_args.loglevel.upper(), None)
if not isinstance(numeric_log_level, int):
    raise ValueError('Invalid log level: %s' % cmd_line_args.loglevel)
logging.basicConfig(format='%(asctime)s : %(threadName)-10s : %(levelname)-7s : %(message)s',
        datefmt='%m-%d-%Y %H:%M:%S',
        level=numeric_log_level,
        filename=logger_filename)

# Implement default of threads*10 queue-length, or assign defined
# We are not allowing a queue depth of 0, thats a hack.
if cmd_line_args.queue_length == 0:
    work_queue_depth = num_worker_threads * 10
else:
    work_queue_depth = cmd_line_args.queue_length

## Done configuring program ##

class Worker(threading.Thread):
    def __init__(self, threadID, name, counter):
        # Thread initialization
        self.threadID = threadID
        threading.Thread.__init__(self)
        self.name = name
        self.counter = counter
        self._stop = threading.Event()

    def run(self):
        global item_no
        global waiting_for_data
        global abort_sub_processes
        logger.info("Starting...")

        # Don't forward CTRL-C signals to subprocesses unless abort_sub_processes is selected.
        def preexec():
            if abort_sub_processes:
                pass
            else:
                setpgrp()

        while not work_queue.empty() or waiting_for_data:
            # Pick an item for processing from the work queue
            # continue if we don't get an item for some time
            try:
                work_item = work_queue.get(timeout=1)
            except Queue.Empty:
                continue

            # Save local- and increment global item number
            thread_lock.acquire()
            local_item = item_no
            item_no += 1
            thread_lock.release()
            logger.info("Processing item : %d"
                    % local_item)

            # Run the processor command, make sure we catch exceptions
            try:
                process = subprocess.Popen([command, work_item], stdin=None, preexec_fn = preexec)
                subprocess.Popen.wait(process)
            except OSError as error:
                logger.critical("Failed executing command: \"%s %s\": %s"
                        % (command, work_item, error.strerror))
            # Check if subprocess exited nonclean
            if process.returncode < 0:
                logger.critical("Return value \"%d\" (Aborted), executing command: \"%s %s\""
                        % (process.returncode, command, work_item))
                logger.critical("Aborting!")
                exit(1)
            elif process.returncode != 0:
                logger.warning("Return value \"%d\", executing command: \"%s %s\""
                        % (process.returncode, command, work_item))

            # Report processing task done
            work_queue.task_done()
            logger.info("Finished item : %d" % local_item)
            # Check if we have been aborted
            if self.stopped():
                logger.critical("Aborting!")
                exit(1)

        # Thread finished
        logger.info("Finished.")

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

def start_workers(num_worker_threads):
    workers = []

    # Create new threads
    for worker_id in range(num_worker_threads):
        workers.append(Worker(worker_id, "Worker-%d" % worker_id, worker_id))

    # Start the threads
    for worker_id in range(num_worker_threads):
        workers[worker_id].start()
    return workers

def abort_workers(num_worker_threads):
    global abort_sub_processes
    logger.critical("Aborting! (Keyboard Interrupt), signalling abort to worker threads.")

    # Print if we are forwarding SIGINT to sub processes
    if abort_sub_processes:
        logger.warning("Forwarding SIGINT signal to sub processes.")
    else:
        logger.warning("Not forwarding SIGINT signal to sub processes, waiting for them to finish")

    # Send stop signal to all workers
    for worker_id in range(num_worker_threads):
        logger.warning("Sending abort signal to Worker-%d" % worker_id)
        workers[worker_id].stop()

def wait_for_workers_to_finish():
    global aborting

    # Artificial number, used to start waiting loop
    threads_alive = 1

    logger.info("Waiting for all workers to finish..")
    while threads_alive > 0:
        threads_alive = 0
        for thread in workers:
            if thread.isAlive():
                threads_alive += 1
        if threads_alive > 0:
            sleep(1)
            if aborting:
                logger.warning("Waiting for %d workers(s) to finish" % threads_alive)

def reader():
    global aborting
    logger.info("Starting reader...")

    # Start reading from input
    while 1:
        try:
            input_line = stdin.readline()
        except KeyboardInterrupt:
            logger.critical("Aborting! (Keyboard Interrupt)")
            aborting = True
            break
        if not input_line:
            logger.info("Finished processing input.")
            break

        # Add item to the work queue
        work_queue.put(input_line.rstrip())

        # Write status
        logger.info("Added item, queue length : %d" % work_queue.qsize())
        if work_queue.qsize() == work_queue_depth:
            logger.debug("Queue full, waiting for workers")

try:
    # Internal variables
    thread_lock = threading.Lock() # Thread sync mutex
    item_no = 0                    # Item number, used for simple item acounting in printouts
    waiting_for_data = True        # Are we still expecting input?
    aborting = False               # Are we aborting?

    # Hello world..
    logging.info("Starting...")

    # Create work queue
    work_queue = Queue.Queue(work_queue_depth)
    logger.info("Work queue created, max length: %d" % work_queue_depth)

    # Start worker threads
    workers = start_workers(num_worker_threads)

    # Start reading input data
    reader()

    # At this point we are not getting any more data
    # - workers are listening for this signal to shut down
    waiting_for_data = False

    # If we received an abort signal while reading, abort workers
    if aborting:
        abort_workers(num_worker_threads)
    
    wait_for_workers_to_finish()

except KeyboardInterrupt:
    aborting = True
    abort_workers(num_worker_threads)
    wait_for_workers_to_finish()

finally:
    # All done, lets do a sane exit
    if aborting:
        logger.critical("All workers aborted, exiting.")
        exit(1)
    else:
        logger.info("All workers finished, exiting.")
        exit(0)
