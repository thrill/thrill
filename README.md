# c7a
THE code repository for the almighty c7a-framework.

- <a href="https://github.com/PdF14-MR/c7a/wiki/Styleguide">Styleguide</a> ist im wiki.
- Unit-test schreiben? Lies <a href="https://code.google.com/p/googletest/wiki/Primer#Simple_Tests">hier</a>

Coding camps are times of anarchy. Anarchy does not need to stick to workflow. :rage:

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
