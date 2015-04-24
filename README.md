# c7a
THE code repository for the allmighty c7a-framework.

Coding camps are times of anarchy. Anarchy does not need to stick to workflow.

# Workflow
1. Assign JIRA ticket to yourself & pull to 'In Progress'
2. Code & commit to branch CA-XXX/name-of-the-ticket
3. Run fancy code clean scripts
4. Push feature branch to remote
5. Check result of build server
6. Notify reviewer (assign JIRA ticket, move to 'In Review')
7. Reviewer commits small changes to feature branch (GOTO 4), bounces big changes back to you and re-assigns ticket to you (GOTO 2)
9. Let build server commit feature branch to master

# Definition of Done
- build server is happy
- at least one reviewer is happy
- no tests have been removed (better: new features covered by tests)
- 100% code compile coverage

# Halp! I cannot push my feature branch to master
1. Fetch the latest version of master branch
2. Checkout your feature branch
3. ```git merge master```
4. resolve merge conflicts
5. push your feature branch to remote
6. wait for build server to be happy
7. let build server merge to master
