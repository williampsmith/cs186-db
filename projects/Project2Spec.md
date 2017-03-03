# Project 2: B+ Trees

## Logistics

**Due date: Saturday 3/4/17, 11:59:59 PM**

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

In this project, you will be implementing a B+ tree. This project builds on top
of the code and functionality that you should be familiar with from Project 1.

We will release the staff solutions for Project 1 to a private repository on
Tuesday, February 21. You should be able to use your own solutions for Project
1, but the staff solutions exists as a reference to be integrated if need be.
Regardless, a correct working implementation of Project 1 is **necessary** in
order to complete this project.

## Getting Started

As usual, you can get the Project 2 skeleton, spec, and tests from the [Github
course repository](https://github.com/berkeley-cs186/course) by running `git
pull course master`, assuming you've completed Homework 0 set up.

**You can also find all of the API documentation of the code
[here](http://www.cs186berkeley.net/projects/).**

You should see `TestLargeBPlusTree` and `TestSmallBPlusTree` failing after
running `mvn clean compile && mvn test` on the Project 2 skeleton with your
Project 1 solutions.

### Setup

All of the Java, Maven, and IntelliJ setup from Project 1 is still applicable
for this project. Assuming that you were able to complete Project 1, you should
not need to do any additional setup for this project. Refer to the Project 1
spec for setup details.

To learn how to run the debugger in IntelliJ, go to Piazza post
[@559](https://piazza.com/class/ixw7vu9jiqb2br?cid=559).

### Autograding and Submission

You can run our autograder starting on *Saturday, February 25*:

    $ git push origin master:ag/proj2

Please run this sparingly as it only runs `mvn clean compile && mvn test`,
which you can do locally. Treat this as a sanity check.

**Do not rename any of your directories pulled from the `course` repo.**

Occasionally we might release an update to assignment files:

    $ git pull course master

Finally, when you're satisfied with your submission, submit it:

    $ git push origin master
    $ git push origin master:submit/proj2

Once you've submitted Project 2, you can confirm by checking that your latest
code has been pushed to the `submit/proj2` branch on Github.

## Starter Code

### Package Overview

There's a bunch of code that we've provided for you, both from Project 1 and
some new additions in this project. We strongly suggest that you spend some
time looking through it before starting to write your own code. The [Project 1
spec](Project1Spec.md#package-overview) contains descriptions for each package
of the codebase, but the relevant parts for this project are all in the `index`
package. Briefly, it consists of a `BPlusTree` class that comprises of
`BPlusNode`s, which `InnerNode`s and `LeafNode`s subclass from. Finally, each
node contains entries. Read over all the classes in the `index` package.

All of the starter code descriptions from Project 1 are still valid. We have
modified some classes and added additional functionality, but for the most part
the code is the same except where we have specifically noted differences here
in this document.

## Your Assignment

### Part 1: B+ Trees

#### 1.1 Inserting Keys

You will first implement B+ tree key insertion. In order to support key
insertion, you will first have to implement the methods `BPlusTree#insertKey`,
`InnerNode#insertBEntry`, and `LeafNode#insertBEntry`.

Recall that if a node is full when inserting a key, you will need to split the
node. Implement the `InnerNode#splitNode` and `LeafNode#splitNode` methods.
Remember to keep the B+ tree key invariance and that when dealing with splits,
we copy keys up from the leaf node and push keys up from the inner node.

Be sure to call `BPlusTree#updateRoot` if the root node has been split. The
root node may be either a `LeafNode` or `InnerNode`.

#### 1.2 Iterators

Similar to Project 1, you will implement the `BPlusIterator` subclass of
`BPlusTree`. This iterator will be a little different from the `TableIterator`,
however. You will need to support equality lookups, bounded range lookups, and
full index scans with this iterator, which are respectively called from
`BPlusTree#lookupKey`, `BPlusTree#sortedScanFrom`, and `BPlusTree#sortedScan`.
Think about how to reuse your code for each case.

### 1.3 Testing

If you've completed all the sections up to this point, you should now be
passing **all** of the tests that we've given you. Again, we strongly encourage
you to write tests as you go to try to catch any relevant bugs in your
implementation.

### Part 2: Testing

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
`@Category(StudentTestP2.class)` annotations. Note that this is different from
the `@Category(StudentTest.class)` annotation you used in Project 1! This is
important in making sure that your Project 2 tests are not mixed up with your
Project 1 tests. We have included an example test in the `TestLargeBPlusTree`
class:
```
@Test
@Category(StudentTestP2.class)
public void testSample() {
  assertEquals(true, true); // Do not actually write a test like this!
}
```

Then whenever you run `mvn test`, your test will be run as well. To run only
the tests that you wrote for this project, you may run `mvn test
-Dtest=StudentTestSuiteP2`. You now also have the ability to run only the tests
in a specific package. For example, you may run `mvn test
-Dtest="edu.berkeley.cs186.database.index.*"` to run all of the tests in the
`index` package. This may be helpful for debugging to save you time from
running the whole test suite.

### Part 3: Feedback

We've been working really hard to give you guys the best experience possible
with these new projects. We'd love to improve on them and make sure we're
giving reasonable assignments that are helping you learn. In that vein, please
fill out [this Google
Form](https://docs.google.com/forms/d/e/1FAIpQLSc57bY8k_365swmsLUN07P0UE49wK9lgrl1Ig-5lOtpu1mFbQ/viewform)
to help us understand how we're doing!
