# Project 3: Query Operators

## Logistics

**Due date: Friday 4/7/17, 11:59:59 PM**

### Grading

This project is worth **10%** of your overall course grade. Your project grade
will be out of 100 points:

* 60 points for passing all of the tests provided for you. All tests are in
  `src/test/java/edu/berkeley/cs186/database`, so you can run them as you write
  code and inspect the tests to debug. Our testing provides extensive unit
  testing and some integration (end-to-end) testing.
* 10 points for writing your own, valid tests (10 tests total, 1 point each).
  The tests **must** pass both your implementation and the staff solution to be
  considered valid tests.
* 30 points for passing additional hidden tests.

## Background

In this project, you will be implementing five query operators. This project
builds on top of the code and functionality that you should be familiar with
from Project 1 and Project 2.

We will release the staff solutions for Project 2 to a private repository on
Saturday, March 18. You should be able to use your own solutions for Project
1 and Project 2, but the staff solutions exists as a reference to be integrated if need be.
Regardless, a correct working implementation of Project 1 and Project 2 is **necessary** in
order to complete this project.

## Getting Started

As usual, you can get the Project 3 skeleton, spec, and tests from the [Github
course repository](https://github.com/berkeley-cs186/course) by running `git
pull course master`, assuming you've completed Homework 0 set up.

**You can also find all of the API documentation of the code
[here](http://www.cs186berkeley.net/projects/).**

You should see `Tests run: 168, Failures: 8, Errors: 0, Skipped: 0` after
running `mvn clean compile && mvn test` after getting project 3 skeleton code and having a correct working implementation of project 1 and project 2. This should be your starting point of project 3.

### Setup

All of the Java, Maven, and IntelliJ setup from Project 1 is still applicable
for this project. Assuming that you were able to complete Project 1, you should
not need to do any additional setup for this project. Refer to the Project 1
spec for setup details.

To learn how to run the debugger in IntelliJ, go to Piazza post
[@559](https://piazza.com/class/ixw7vu9jiqb2br?cid=559).

### Autograding and Submission

You can run our autograder starting on *Saturday, March 25*:

    $ git push origin master:ag/proj3

Please run this sparingly as it only runs `mvn clean compile && mvn test`,
which you can do locally. Treat this as a sanity check.

**Do not rename any of your directories pulled from the `course` repo.**

Occasionally we might release an update to assignment files:

    $ git pull course master

Finally, when you're satisfied with your submission, submit it:

    $ git push origin master
    $ git push origin master:submit/proj3

Once you've submitted Project 3, you can confirm by checking that your latest
code has been pushed to the `submit/proj3` branch on Github.

## Starter Code

### Package Overview

There's a bunch of code that we've provided for you, both from Project 1, Project 2 and
some new additions in this project. We strongly suggest that you spend some
time looking through it before starting to write your own code. The [Project 1
spec](Project1Spec.md#package-overview) contains descriptions for each package
of the codebase, but the relevant parts for this project are all in the `query`
package. Read over all the classes in the `query` package.

All of the starter code descriptions from Project 1 and Project 2 are still valid. We have
modified some classes and added additional functionality, but for the most part
the code is the same except where we have specifically noted differences here
in this document.

## Your Assignment

For this project, you will implement four join operators: the Page Nested Loop
Join, Block Nested Loop Join, Grace Hash Join, and Sort Merge Join operators. The relevant classes
are `PNLJOperator`, `BNLJOperator`, `GraceHashOperator`, and `SortMergeOperator` respectively.

In addition, you will also implement the index scan operator, which iterates over an index
given a predicate. The relevant class for this is `IndexScanOperator`.

Thus, you will implement five operators in total.

## Important Points
* Do not materialize the entire output relation, i.e. all records, within an iterator.
* We suggest to start with `PNLJOperator`. We've provided you with some suggested fields.
* The order of a join output will differ depending on the join algorithm used.
We will be checking the expected order of the output for each join.
As such, DO NOT USE THE SAME CODE FOR ALL JOINS - you will be penalized heavily if you do.
* For Block Nested Loop Join, you are given the number of buffer pages in your buffer
pool as `numBuffers`.
* For Grace Hash Join, you should use the `hashCode()` method in the `Databox` class.
* For Grace Hash Join, build and probe on the left relation instead of the
smaller relation. Note that this is different from what has been presented in lecture.
* For Sort Merge Join, you may assume that you will never need to "backtrack"
to a previous page during the merge operation (see Lecture 2/21 slides 62 and
63). You still need to account for the "backtrack" behavior in the algorithm,
but assume you will never need to read in a previous page.
* For Sort Merge Join, you do not need to implement external sorting of the
individual relations being joined. You should write your own function that
sorts all records (i.e. `Collections.sort`) and writes them to a temporary
table.
* You may assume that underlying relations will not be modified while the result of their join
operation is being iterated over.

### Part 1: Query Operators

All join operators are subclasses of `JoinOperator`, and all operators are subclasses of `QueryOperator`.
We have provided `SelectOperator`, `GroupByOperator`, `SequentialScanOperator`, and `SNLJOperator` as references.

#### 1.1 Join Operators

First, take a look at `SNLJOperator` to understand how join operators are implemented.
Then, implement the iterators for `PNLJOperator`, `BNLJOperator`, `GraceHashOperator`, and
`SortMergeOperator` as described in lecture. It is recommended you review the lecture slides
before getting down and dirty with the code. Think carefully about what instance variables you need before you begin!

For `SortMergeOperator`, you may want to consider implementing a helper function that sorts
a single relation (though not required).

#### 1.2 Index Scan Operator

Next, implement an iterator for `IndexScanOperator` that supports the predicates (`EQUALS`,
 `LESS_THAN`, `LESS_THAN_EQUALS`, `GREATER_THAN`, `GREATER_THAN_EQUALS`) on
the given index.
These predicates are part of the enum `PredicateOperator` found in the class `QueryPlan`.

Update: You don't have to implement `NOT_EQUALS`; if you have already done,
you get 1 extra credit.

### Part 2: Testing

If you've completed all the sections up to this point, you should now be
passing **all** of the tests that we've given you. Again, we strongly encourage
you to write tests as you go to try to catch any relevant bugs in your
implementation.

We can't emphasize enough how important it is to test your code! Like we said
earlier, writing valid tests that **test actual code** (i.e., don't write
`assertEquals(true, true);`, or we'll be mad at you) is worth 10% of your
project grade.

CS 186 is a design course, and validating the reasonability and the
functionality of the code you've designed and implemented is very important. We
suggest you try to find the trickiest edge cases you can and expose them with
your tests. Testing that your code does exactly what you expect in the simplest
case is good for sanity's sake, but it's often not going to be where the bugs
are.

#### Writing Tests

In the `src/test` directory you'll notice we've included several tests for you
already. You should take a look at these to get a sense of how to write tests.
You should write your tests in one of the existing files according to the
functionality you're trying to test.

All test methods you write should have both the `@Test` and
`@Category(StudentTestP3.class)` annotations. Note that this is different from
the `@Category(StudentTest.class)` annotation you used in Project 1! This is
important in making sure that your Project 3 tests are not mixed up with your
Project 1 tests. We have included an example test in the `TestJoinOperator`
class:
```
@Test
@Category(StudentTestP3.class)
public void testSample() {
  assertEquals(true, true); // Do not actually write a test like this!
}
```

Then whenever you run `mvn test`, your test will be run as well. To run only
the tests that you wrote for this project, you may run `mvn test
-Dtest=StudentTestSuiteP3`. You now also have the ability to run only the tests
in a specific package. For example, you may run `mvn test
-Dtest="edu.berkeley.cs186.database.query.*"` to run all of the tests in the
`query` package. This may be helpful for debugging to save you time from
running the whole test suite.

### Part 3: Feedback

We've been working really hard to give you guys the best experience possible
with these new projects. We'd love to improve on them and make sure we're
giving reasonable assignments that are helping you learn. In that vein, please
fill out [this Google
Form](https://drive.google.com/open?id=1mo0Y85qhbXL6340pK0prjCbVX0BsHh3W76kfvxTa5fc)
to help us understand how we're doing!
