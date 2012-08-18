# tQueue

tQueue is a generic multithreaded command queuer written in Python.

## Features

 * configurable amount of worker threads
 * configurable work queue size
 * configurable loglevel
 * configurable handling of aborts; can let workers finish current task before shutting down, or shut them down immediatly
 * handles processor script return codes and errors.

## Getting Started

 * Checkout the source: "git clone git://github.com/saaby/tqueue.git" and install it yourself.

## Simplest usecase
    $ generate_item_list | ./tqueue.py processor_script.sh

Which will process each line from the generator as an argument to the processor_script.sh (running 4 parallel scripts at a time, which is default)
