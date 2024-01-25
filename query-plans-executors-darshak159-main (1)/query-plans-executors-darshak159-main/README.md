[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/RrAt6wHd)
[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12407743&assignment_repo_type=AssignmentRepo)
# 4P32-Executors

 Assignment 3: Implement Sequential Scan, Nested Loop Join and Aggregation Operators.
---
## Goal

You will implement the function "fetchNext()" of the Sequential Scan, Nested Loop Join and Aggregation Operators.
The operators implements the pipelining tuple-at-a-time query processing model. (Be aware that some operators are non-blocking and others are blocking)

---
## Overview

Given a query plan that is passed to the execution engine, the execution engine transforms it into an execution plan and runs it using the specified operators.

Once you have implemented the operators, we will be able to run some type of query plans. For example, you will be able to run the query: give me the total number of movies that each male actors has played.

`SELECT COUNT(movie_id)
FROM actors, roles
WHERE actors.gender = M
GROUPBY actor_id`


This query is parsed into the query plan:

*AggregationPlanNode(NestedLoopJoinPlanNode(SeqScanPlanNode(actor), SeqScanPlanNode(movies)*


The query plan is then transformed

*AggregationExecutor(NestedLoopJoinExecutor(SeqScanExecutor(actor), SeqScanExecutor(movies)*

The operators implements the pipelining tuple-at-a-time query processing model.
The code return the results of the query

### First Step

Using the computer that you normally use for your school work (e.g., laptop), create an account on GitHub (if you do not already have one).  You may need to install git on it. Checkout your HW3-Executors repository. 

### Exercise

We are using the IMDB data as we did for assigment 1.

You are going to execute the query plans for the following queries:
* Select all males actors from the table actors
* Select the roles for all actors from the table roles
* Select all males actors and their roles (join tables actors and roles on actor_id)
* Count total of roles of each actor


Please implement the "fetchNext()" functions in the file exercutors.py. Please also complete the exercise given in "exercise.py"

Do not forget to "git commit" your changes to the code. Once you are finished, "git push" your code to the Git Classroom