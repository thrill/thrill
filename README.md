# Thrill - A Distributed Big Data Batch Processing Framework in C++

THE code repository for the almighty Thrill-framework.

- Travis-CI Status [![Travis-CI Status](https://travis-ci.org/thrill/thrill.svg?branch=master)](https://travis-ci.org/thrill/thrill)
- Jenkins Status [![Jenkins Status](http://i10login.iti.kit.edu:8080/buildStatus/icon?job=Thrill)](http://i10login.iti.kit.edu:8080/job/Thrill/)
- [Live Doxygen Documentation](http://i10login.iti.kit.edu/thrill-doxygen/)
- [Testsuite Code Coverage](http://i10login.iti.kit.edu/thrill-coverage/)
- [Coding Style Guidelines](http://i10login.iti.kit.edu/thrill-doxygen/style_guide.html)
- Write google tests? Read <a href="https://code.google.com/p/googletest/wiki/Primer#Simple_Tests">here</a>

Coding camps are times of anarchy. Anarchy does not need to stick to workflow. :rage:

# Directory Structure

- compile all code during build tests!
- no duplication of example code!

## examples
- contains API examples for the future users.
- e.g. full code of wordcount and page rank for them to learn from.

## tests
- contains unit tests
- contains integration? tests that ensure that all API examples (the code therein! not a copy) works correctly.
- internal example program (e.g. cmdline parser)
- performance tests. e.g. hash table never runs slower than unordered map. run on EVERY! test run.

## benchmarks
- only the scripts to run and plot the benchmarks. no results.
- code internal micro-benchmarks, e.g. hash table.
- code of internal micro-benchmarks must be compiled during build testing, but not run!

# Temporary Workflow
- steps 1 - 5 (see below)
- merge into master and push

# Workflow
1. Assign JIRA ticket to yourself & pull to 'In Progress' :point_left:
2. Code & commit to branch CA-XXX/name-of-the-ticket
3. Run fancy code clean scripts
  * run `perl misc/analyze-source.pl` from source root, READ the output.
  * run `perl misc/analyze-source.pl -w` to write changes.
4. Push feature branch to remote
5. Check result of build server
6. Notify reviewer (assign JIRA ticket, move to 'In Review') :eyes:
7. Reviewer commits small changes to feature branch (GOTO 4), bounces big changes back to you and re-assigns ticket to you (GOTO 2) :punch:
9. Let build server commit feature branch to master :ok_hand:

# Definition of Done :heavy_check_mark:
- build server is happy
- changes have been documented (if necessary)
- at least one reviewer saw it and is happy (more happy reviewers are better :two_hearts:)
- no tests have been removed (better: new features covered by tests)
- 100% code compile coverage

# Halp! I cannot push my feature branch to master :fire:
1. Fetch the latest version of master branch
2. Checkout your feature branch
3. ```git merge master```
4. resolve merge conflicts
5. push your feature branch to remote
6. wait for build server to be happy
7. let build server merge to master
