package edu.berkeley.cs186.database.query;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.Rule;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP3;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import static org.junit.Assert.*;

public class TestProjectOperator {

  @Rule
  public Timeout globalTimeout = Timeout.seconds(1); // 1 seconds max per method tested

  @Test
  public void testSelectOperatorSchema() throws QueryPlanException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");

    ProjectOperator projectOperator = new ProjectOperator(sourceOperator, columnNames, false, null, null);

    List<String> outputSchemaNames = new ArrayList<String>();
    outputSchemaNames.add("int");
    List<DataBox> outputSchemaTypes = new ArrayList<DataBox>();
    outputSchemaTypes.add(new IntDataBox());
    Schema expectedSchema = new Schema(outputSchemaNames, outputSchemaTypes);

    assertEquals(expectedSchema, projectOperator.getOutputSchema());
  }

  @Test
  public void testProjectCorrectColumns() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");

    ProjectOperator projectOperator = new ProjectOperator(sourceOperator, columnNames, false, null, null);

    Iterator<Record> outputRecords = projectOperator.execute();
    List<Record> recordList = new ArrayList<Record>();

    while (outputRecords.hasNext()) {
      recordList.add(outputRecords.next());
    }

    for (int i = 0; i < 100; i++) {
      assertEquals(1, recordList.get(i).getValues().get(0).getInt());
    }
  }

  @Test
  public void testProjectMultipleColumns() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    columnNames.add("string");

    ProjectOperator projectOperator = new ProjectOperator(sourceOperator, columnNames, false, null, null);

    Iterator<Record> outputRecords = projectOperator.execute();
    List<Record> recordList = new ArrayList<Record>();

    while (outputRecords.hasNext()) {
      recordList.add(outputRecords.next());
    }

    for (int i = 0; i < 100; i++) {
      assertEquals(1, recordList.get(i).getValues().get(0).getInt());
      assertEquals("abcde", recordList.get(i).getValues().get(1).getString());
    }
  }

  @Test(expected = QueryPlanException.class)
  public void testProjectNonexistentColumn() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("nonexistentColumn");

    new ProjectOperator(sourceOperator, columnNames, false, null, null);
  }

  @Test
  public void testProjectCount() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    List<String> columnNames = new ArrayList<String>();

    ProjectOperator projectOperator = new ProjectOperator(sourceOperator, columnNames, true, null, null);

    Iterator<Record> outputRecords = projectOperator.execute();
    List<Record> recordList = new ArrayList<Record>();

    List<String> outputSchemaNames = new ArrayList<String>();
    outputSchemaNames.add("sumAgg");
    List<DataBox> outputSchemaTypes = new ArrayList<DataBox>();
    outputSchemaTypes.add(new IntDataBox());
    Schema expectedSchema = new Schema(outputSchemaNames, outputSchemaTypes);

    assertEquals(expectedSchema, projectOperator.getOutputSchema());

    while (outputRecords.hasNext()) {
      recordList.add(outputRecords.next());
    }

    assertEquals(1, recordList.size());
    assertEquals(100, recordList.get(0).getValues().get(0).getInt());
  }

  @Test
  public void testProjectIntegerSum() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    List<String> columnNames = new ArrayList<String>();

    ProjectOperator projectOperator = new ProjectOperator(sourceOperator, columnNames, false, null, "int");

    Iterator<Record> outputRecords = projectOperator.execute();
    List<Record> recordList = new ArrayList<Record>();

    List<String> outputSchemaNames = new ArrayList<String>();
    outputSchemaNames.add("countAgg");
    List<DataBox> outputSchemaTypes = new ArrayList<DataBox>();
    outputSchemaTypes.add(new IntDataBox());
    Schema expectedSchema = new Schema(outputSchemaNames, outputSchemaTypes);

    assertEquals(expectedSchema, projectOperator.getOutputSchema());

    while (outputRecords.hasNext()) {
      recordList.add(outputRecords.next());
    }

    assertEquals(1, recordList.size());
    assertEquals(100, recordList.get(0).getValues().get(0).getInt());
  }

  @Test
  public void testProjectAverage() throws QueryPlanException, DatabaseException {
    List<Integer> values = new ArrayList<Integer>();
    int sum = 0;

    for (int i = 0; i < 100; i++) {
      sum += i;
      values.add(i);
    }

    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);
    ProjectOperator projectOperator =  new ProjectOperator(sourceOperator, new ArrayList<String>(), false, "int", null);
    Iterator<Record> outputRecords = projectOperator.execute();

    List<String> outputSchemaNames = new ArrayList<String>();
    outputSchemaNames.add("averageAgg");
    List<DataBox> outputSchemaTypes = new ArrayList<DataBox>();
    outputSchemaTypes.add(new FloatDataBox());
    Schema expectedSchema = new Schema(outputSchemaNames, outputSchemaTypes);

    assertEquals(expectedSchema, projectOperator.getOutputSchema());

    List<Record> recordList = new ArrayList<Record>();
    while (outputRecords.hasNext()) {
      recordList.add(outputRecords.next());
    }

    assertEquals(1, recordList.size());

    // the last argument specifies the precision to which the answer should be accurate
    assertEquals(((float) sum)/ 100, recordList.get(0).getValues().get(0).getFloat(), 0.01);
  }

  @Test
  public void testProjectFloatSum() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    List<String> columnNames = new ArrayList<String>();

    ProjectOperator projectOperator = new ProjectOperator(sourceOperator, columnNames, false, null, "float");

    Iterator<Record> outputRecords = projectOperator.execute();
    List<Record> recordList = new ArrayList<Record>();

    List<String> outputSchemaNames = new ArrayList<String>();
    outputSchemaNames.add("sumAgg");
    List<DataBox> outputSchemaTypes = new ArrayList<DataBox>();
    outputSchemaTypes.add(new FloatDataBox());
    Schema expectedSchema = new Schema(outputSchemaNames, outputSchemaTypes);

    assertEquals(expectedSchema, projectOperator.getOutputSchema());

    while (outputRecords.hasNext()) {
      recordList.add(outputRecords.next());
    }

    assertEquals(1, recordList.size());
    assertEquals(1.2 * 100f, recordList.get(0).getValues().get(0).getFloat(), 0.01);
  }

  @Test
  public void testProjectAllAggregates() throws QueryPlanException, DatabaseException {
    List<Integer> values = new ArrayList<Integer>();
    int sum = 0;

    for (int i = 0; i < 100; i++) {
      sum += i;
      values.add(i);
    }

    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);
    ProjectOperator projectOperator =  new ProjectOperator(sourceOperator, new ArrayList<String>(), true, "int", "int");
    Iterator<Record> outputRecords = projectOperator.execute();

    List<String> outputSchemaNames = new ArrayList<String>();
    outputSchemaNames.add("countAgg");
    outputSchemaNames.add("sumAgg");
    outputSchemaNames.add("averageAgg");
    List<DataBox> outputSchemaTypes = new ArrayList<DataBox>();
    outputSchemaTypes.add(new IntDataBox());
    outputSchemaTypes.add(new IntDataBox());
    outputSchemaTypes.add(new FloatDataBox());
    Schema expectedSchema = new Schema(outputSchemaNames, outputSchemaTypes);

    assertEquals(expectedSchema, projectOperator.getOutputSchema());

    List<Record> recordList = new ArrayList<Record>();
    while (outputRecords.hasNext()) {
      recordList.add(outputRecords.next());
    }

    assertEquals(1, recordList.size());

    // the last argument specifies the precision to which the answer should be accurate
    assertEquals(100, recordList.get(0).getValues().get(0).getInt());
    assertEquals(sum, recordList.get(0).getValues().get(1).getInt());
    assertEquals(((float) sum)/ 100, recordList.get(0).getValues().get(2).getFloat(), 0.01);
  }

  @Test(expected = QueryPlanException.class)
  public void testProjectNonexistentColumnAggregate() throws QueryPlanException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    List<String> columnNames = new ArrayList<String>();

    new ProjectOperator(sourceOperator, columnNames, false, null, "nonexistentColumn");
  }
}
