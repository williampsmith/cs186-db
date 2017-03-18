package edu.berkeley.cs186.database.query;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP3;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.table.Record;

import static org.junit.Assert.*;

public class TestSelectOperator {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(1); // 1 seconds max per method tested

  @Test
  public void testWhereOperatorSchema() throws QueryPlanException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    SelectOperator selectOperator = new SelectOperator(sourceOperator, "int",
            QueryPlan.PredicateOperator.EQUALS, new IntDataBox(1));

    assertEquals(TestUtils.createSchemaWithAllTypes(), selectOperator.getOutputSchema());
  }

  @Test
  public void testSelectFiltersCorrectRecords() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    SelectOperator selectOperator = new SelectOperator(sourceOperator, "int",
            QueryPlan.PredicateOperator.EQUALS, new IntDataBox(1));

    Iterator<Record> output = selectOperator.execute();
    List<Record> outputList = new ArrayList<Record>();

    while (output.hasNext()) {
      outputList.add(output.next());
    }

    assertEquals(100, outputList.size());

    Record inputRecord = TestUtils.createRecordWithAllTypes();
    for (Record record : outputList) {
      assertEquals(inputRecord, record);
    }
  }

  @Test
  public void testSelectRemovesIncorrectRecords() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    SelectOperator selectOperator = new SelectOperator(sourceOperator, "int",
            QueryPlan.PredicateOperator.EQUALS, new IntDataBox(10));

    Iterator<Record> output = selectOperator.execute();
    List<Record> outputList = new ArrayList<Record>();

    while (output.hasNext()) {
      outputList.add(output.next());
    }

    assertEquals(0, outputList.size());
  }

  @Test
  public void testSelectRemovesSomeRecords() throws QueryPlanException, DatabaseException {
    List<Integer> values = new ArrayList<Integer>();
    values.add(1);
    values.add(2);
    values.add(3);
    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

    List<DataBox> dataTypeValues = new ArrayList<DataBox>();
    dataTypeValues.add(new IntDataBox(1));
    Record keptRecord = new Record(dataTypeValues);

    SelectOperator selectOperator = new SelectOperator(sourceOperator, "int",
            QueryPlan.PredicateOperator.EQUALS, new IntDataBox(1));

    Iterator<Record> output = selectOperator.execute();
    List<Record> outputList = new ArrayList<Record>();

    while (output.hasNext()) {
      outputList.add(output.next());
    }

    assertEquals(1, outputList.size());

    assertEquals(keptRecord, outputList.get(0));
  }

  @Test(expected = QueryPlanException.class)
  public void testSelectFailsOnInvalidField() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    new SelectOperator(sourceOperator, "nonexistentField",
            QueryPlan.PredicateOperator.EQUALS, new IntDataBox(10));
  }

  @Test
  public void testSelectNotEquals() throws QueryPlanException, DatabaseException {
    List<Integer> values = new ArrayList<Integer>();
    values.add(1);
    values.add(2);
    values.add(3);
    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

    SelectOperator selectOperator = new SelectOperator(sourceOperator, "int",
            QueryPlan.PredicateOperator.NOT_EQUALS, new IntDataBox(1));

    Iterator<Record> output = selectOperator.execute();
    List<Record> outputList = new ArrayList<Record>();

    while (output.hasNext()) {
      outputList.add(output.next());
    }

    assertEquals(2, outputList.size());

    Set<Integer> keptValues = new HashSet<Integer>();
    keptValues.add(2);
    keptValues.add(3);

    for (Record record : outputList) {
      int val = record.getValues().get(0).getInt();
      assert(keptValues).contains(val);
      keptValues.remove(val);
    }

    assertEquals(0, keptValues.size());
  }

  @Test
  public void testSelectLessThan() throws QueryPlanException, DatabaseException {
    List<Integer> values = new ArrayList<Integer>();
    values.add(1);
    values.add(2);
    values.add(3);
    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

    SelectOperator selectOperator = new SelectOperator(sourceOperator, "int",
            QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(3));

    Iterator<Record> output = selectOperator.execute();
    List<Record> outputList = new ArrayList<Record>();

    while (output.hasNext()) {
      outputList.add(output.next());
    }

    assertEquals(2, outputList.size());

    Set<Integer> keptValues = new HashSet<Integer>();
    keptValues.add(1);
    keptValues.add(2);

    for (Record record : outputList) {
      int val = record.getValues().get(0).getInt();
      assert(keptValues).contains(val);
      keptValues.remove(val);
    }

    assertEquals(0, keptValues.size());
  }

  @Test
  public void testSelectGreaterThan() throws QueryPlanException, DatabaseException {
    List<Integer> values = new ArrayList<Integer>();
    values.add(1);
    values.add(2);
    values.add(3);
    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

    SelectOperator selectOperator = new SelectOperator(sourceOperator, "int",
            QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(3));

    Iterator<Record> output = selectOperator.execute();
    List<Record> outputList = new ArrayList<Record>();

    while (output.hasNext()) {
      outputList.add(output.next());
    }

    assertEquals(0, outputList.size());
  }

  @Test
  public void testSelectLessThanEquals() throws QueryPlanException, DatabaseException {
    List<Integer> values = new ArrayList<Integer>();
    values.add(1);
    values.add(2);
    values.add(3);
    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

    SelectOperator selectOperator = new SelectOperator(sourceOperator, "int",
            QueryPlan.PredicateOperator.LESS_THAN_EQUALS, new IntDataBox(3));

    Iterator<Record> output = selectOperator.execute();
    List<Record> outputList = new ArrayList<Record>();

    while (output.hasNext()) {
      outputList.add(output.next());
    }

    assertEquals(3, outputList.size());

    Set<Integer> keptValues = new HashSet<Integer>();
    keptValues.add(1);
    keptValues.add(2);
    keptValues.add(3);

    for (Record record : outputList) {
      int val = record.getValues().get(0).getInt();
      assert(keptValues).contains(val);
      keptValues.remove(val);
    }

    assertEquals(0, keptValues.size());
  }

  @Test
  public void testSelectGreaterThanEquals() throws QueryPlanException, DatabaseException {
    List<Integer> values = new ArrayList<Integer>();
    values.add(1);
    values.add(2);
    values.add(3);
    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

    SelectOperator selectOperator = new SelectOperator(sourceOperator, "int",
            QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(2));

    Iterator<Record> output = selectOperator.execute();
    List<Record> outputList = new ArrayList<Record>();

    while (output.hasNext()) {
      outputList.add(output.next());
    }

    assertEquals(2, outputList.size());

    Set<Integer> keptValues = new HashSet<Integer>();
    keptValues.add(2);
    keptValues.add(3);

    for (Record record : outputList) {
      int val = record.getValues().get(0).getInt();
      assert(keptValues).contains(val);
      keptValues.remove(val);
    }

    assertEquals(0, keptValues.size());
  }
}
