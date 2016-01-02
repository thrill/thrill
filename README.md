# Thrill

Travis-CI Status [![Travis-CI Status](https://travis-ci.org/thrill/thrill.svg?branch=master)](https://travis-ci.org/thrill/thrill)
Jenkins Status [![Jenkins Status](http://i10login.iti.kit.edu:8080/buildStatus/icon?job=Thrill)](http://i10login.iti.kit.edu:8080/job/Thrill)

Thrill is an EXPERIMENTAL C++ framework for distributed Big Data batch computations on a cluster of machines. It is currently being designed and developed as a research project at Karlsruhe Institute of Technology and is in early testing.

[http://project-thrill.org](http://project-thrill.org)

If you'd like to contribute to Thrill, please review the [contribution guidelines](CONTRIBUTING.md).

# Building Thrill

Thrill can be built with ``cmake``. A convenience wrapper combining building and testing is provided, simply run

    ./compile.sh


# Documentation

You can find the latest documentation for Thrill on [Live Doxygen Documentation](http://i10login.iti.kit.edu/thrill-doxygen/).

# Examples

The directory ``./examples`` contains examples for common applications such as WordCount, PageRank and kMeans.

# Tests

The directory ``./tests`` contains unit, integration and performance tests for various components of Thrill.
