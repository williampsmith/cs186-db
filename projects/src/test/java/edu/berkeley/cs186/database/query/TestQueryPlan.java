package edu.berkeley.cs186.database.query;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.table.MarkerRecord;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.StringHistogram;

import static org.junit.Assert.*;

public class TestQueryPlan {
  private Database database;
  private Random random = new Random();
  private String alphabet = StringHistogram.alphaNumeric;
  private String defaulTableName = "testAllTypes";
  private int defaultNumRecords = 100;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public Timeout globalTimeout = Timeout.seconds(10); // 10 seconds max per method tested

  @Before
  public void setUp() throws DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("db");
    this.database = new Database(tempDir.getAbsolutePath());
    this.database.deleteAllTables();
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), this.defaulTableName);
    Database.Transaction transaction = this.database.beginTransaction();

    // by default, create 100 records
    for (int i = 0; i < defaultNumRecords; i++) {
      // generate a random record
      IntDataBox intValue = new IntDataBox(i);
      FloatDataBox floatValue = new FloatDataBox(this.random.nextFloat());
      BoolDataBox boolValue = new BoolDataBox(this.random.nextBoolean());
      String stringValue = "";

      for (int j = 0 ; j < 5; j++) {
        int randomIndex = Math.abs(this.random.nextInt() % alphabet.length());
        stringValue += alphabet.substring(randomIndex, randomIndex + 1);
      }

      List<DataBox> values = new ArrayList<DataBox>();
      values.add(boolValue);
      values.add(intValue);
      values.add(new StringDataBox(stringValue, 5));
      values.add(floatValue);

      transaction.addRecord("testAllTypes", values);
    }

    transaction.end();
  }

  @Test
  public void testSimpleProject() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    columnNames.add("string");

    queryPlan.project(columnNames);
    Iterator<Record> outputIterator = queryPlan.execute();

    int count = 0;
    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();
      assertTrue(record.getValues().get(0) instanceof IntDataBox);
      assertTrue(record.getValues().get(1) instanceof StringDataBox);

      count++;
    }

    assertEquals(this.defaultNumRecords, count);

    transaction.end();
  }

  @Test
  public void testSimpleSelect() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.select("int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(0));

    Iterator<Record> outputIterator = queryPlan.execute();

    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();

      assertTrue(record.getValues().get(1).getInt() >= 0);
    }

    transaction.end();
  }

  @Test
  public void testSimpleGroupBy() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    queryPlan.groupBy("int");
    Iterator<Record> outputIterator = queryPlan.execute();

    boolean first = true;
    int prevValue = 0;
    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();

      if (first) {
        prevValue = record.getValues().get(1).getInt();
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(1).getInt());
      }
    }

    transaction.end();
  }

  @Test
  public void testSimpleJoin() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.join(this.defaulTableName, "int", "int");
    Iterator<Record> outputIterator = queryPlan.execute();

    int count = 0;

    while (outputIterator.hasNext()) {
      count++;

      Record record = outputIterator.next();
      List<DataBox> recordValues = record.getValues();
      assertEquals(recordValues.get(0), recordValues.get(4));
      assertEquals(recordValues.get(1), recordValues.get(5));
      assertEquals(recordValues.get(2), recordValues.get(6));
      assertEquals(recordValues.get(3), recordValues.get(7));
    }

    assertTrue(count >= 100);

    transaction.end();
  }

  @Test
  public void testProjectSelect() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.select("int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(0));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("bool");
    columnNames.add("int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.execute();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());
      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);

      assertTrue(values.get(1).getInt() >= 0);
    }

    transaction.end();
  }

  @Test
  public void testProjectJoin() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.int");
    columnNames.add("t2.string");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.execute();

    int count = 0;
    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof IntDataBox);
      assertTrue(values.get(1) instanceof StringDataBox);

      count++;
    }

    assertTrue(count > 10);

    transaction.end();
  }

  @Test
  public void testSelectJoin() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");

    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.join("t1", "string", "string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));

    Iterator<Record> recordIterator = queryPlan.execute();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(values.get(0), values.get(4));
      assertEquals(values.get(1), values.get(5));
      assertEquals(values.get(2), values.get(6));
      assertEquals(values.get(3), values.get(7));

      assertTrue(values.get(0).getBool());
    }

    transaction.end();
  }

  @Test
  public void testProjectSelectJoin() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");

    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.join("t1", "string", "string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add(this.defaulTableName + ".int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.execute();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);

      assertTrue(values.get(0).getBool());
    }

    transaction.end();
  }

  @Test
  public void testProjectGroupBy() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.execute();

    boolean first = true;
    int prevValue = 0;
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      if (first) {
        prevValue = record.getValues().get(0).getInt();
        assertEquals(1, record.getValues().size());
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(0).getInt());
        assertEquals(1, record.getValues().size());
      }
    }

    transaction.end();
  }

  @Test
  public void testSelectGroupBy() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    queryPlan.select("int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(10));

    Iterator<Record> recordIterator = queryPlan.execute();

    boolean first = true;
    int prevValue = 0;
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      if (first) {
        prevValue = record.getValues().get(1).getInt();
        assertTrue(record.getValues().get(1).getInt() > 10);
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(1).getInt());
        assertTrue(record.getValues().get(1).getInt() > 10);
      }
    }

    transaction.end();
  }

  @Test
  public void testProjectSelectGroupBy() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    queryPlan.select("int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(10));
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("float");
    columnNames.add("int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.execute();

    boolean first = true;
    int prevValue = 0;
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      if (first) {
        prevValue = record.getValues().get(1).getInt();
        assertTrue(record.getValues().get(1).getInt() > 10);
        assertEquals(2, record.getValues().size());
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(1).getInt());
        assertTrue(record.getValues().get(1).getInt() > 10);
        assertEquals(2, record.getValues().size());
      }
    }

    transaction.end();
  }

  @Test
  public void testProjectSelectGroupByJoin() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");

    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.join("t1", "int", "int");
    queryPlan.groupBy("t1.int");
    queryPlan.select(this.defaulTableName + ".int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(10));
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.float");
    columnNames.add(this.defaulTableName + ".int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.execute();

    boolean first = true;
    int prevValue = 0;
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      if (first) {
        prevValue = record.getValues().get(1).getInt();
        assertTrue(record.getValues().get(1).getInt() > 10);
        assertEquals(2, record.getValues().size());
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(1).getInt());
        assertTrue(record.getValues().get(1).getInt() > 10);
        assertEquals(2, record.getValues().size());
      }
    }

    transaction.end();
  }

  @Test
  public void testEmptySelectResult() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.select("int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS,
            new IntDataBox(Integer.MAX_VALUE));

    Iterator<Record> outputIterator = queryPlan.execute();

    int count = 0;
    while (outputIterator.hasNext()) {
      outputIterator.next();
      count++;
    }

    assertEquals(0, count);
    transaction.end();
  }

  @Test
  public void testEmptyJoinResult() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    List<String> otherSchemaNames = new ArrayList<String>();
    otherSchemaNames.add("otherInt");
    List<DataBox> otherSchemaTypes = new ArrayList<DataBox>();
    otherSchemaTypes.add(new IntDataBox());
    Schema otherSchema = new Schema(otherSchemaNames, otherSchemaTypes);

    transaction.createTempTable(otherSchema, "TestOtherTableForEmptyJoin");

    queryPlan.join("TestOtherTableForEmptyJoin", "int", "otherInt");

    Iterator<Record> outputIterator = queryPlan.execute();

    int count = 0;
    while (outputIterator.hasNext()) {
      outputIterator.next();
      count++;
    }

    assertEquals(0, count);
    transaction.end();
  }

  @Test
  public void testIndexEqualityLookup() throws DatabaseException, QueryPlanException {
    List<String> intTableNames = new ArrayList<String>() ;
    intTableNames.add("int");

    List<DataBox> intTableTypes = new ArrayList<DataBox>();
    intTableTypes.add(new IntDataBox());

    this.database.createTableWithIndices(new Schema(intTableNames, intTableTypes), "tempIntTable", intTableNames);

    Database.Transaction transaction = this.database.beginTransaction();

    Record record = null;
    for (int i = 0; i < 100; i++) {
      List<DataBox> values = new ArrayList<DataBox>();
      values.add(new IntDataBox(i));

      transaction.addRecord("tempIntTable", values);

      record = new Record(values);
    }

    QueryPlan queryPlan = transaction.query("tempIntTable");
    queryPlan.select("int", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(99));
    Iterator<Record> result = queryPlan.execute();

    assertEquals(record, result.next());

    assertFalse(result.hasNext());
  }

  @Test
  public void testUseIndexRangeLookup() throws DatabaseException, QueryPlanException {
    List<String> intTableNames = new ArrayList<String>() ;
    intTableNames.add("int");

    List<DataBox> intTableTypes = new ArrayList<DataBox>();
    intTableTypes.add(new IntDataBox());

    this.database.createTableWithIndices(new Schema(intTableNames, intTableTypes), "tempIntTable", intTableNames);

    Database.Transaction transaction = this.database.beginTransaction();

    for (int i = 0; i < 100; i++) {
      List<DataBox> values = new ArrayList<DataBox>();
      values.add(new IntDataBox(i));

      transaction.addRecord("tempIntTable", values);
    }

    QueryPlan queryPlan = transaction.query("tempIntTable");
    queryPlan.select("int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(50));
    Iterator<Record> result = queryPlan.execute();

    int count = 0;
    while (result.hasNext()) {
      assertEquals(count + 50, result.next().getValues().get(0).getInt());

      count++;
    }

    assertEquals(count, 50);
  }

  @Test
  public void testProjectGroupByWithAggregates() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    queryPlan.count();
    queryPlan.average("int");
    queryPlan.sum("int");

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.execute();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(4, values.size());
      int value = values.get(0).getInt();
      int count = values.get(1).getInt();
      int sum = values.get(2).getInt();
      float average = values.get(3).getFloat();

      assertEquals(value * count, sum);

      // this is a high threshold to account for integer to float conversion inaccuracies
      assertEquals(sum / count, average, 10);
    }

    transaction.end();
  }

  @Test(expected = QueryPlanException.class)
  public void testProjectColumnNotInGroupBy() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    List<String> columns = new ArrayList<String>();
    columns.add("string");
    queryPlan.project(columns);

    queryPlan.execute();

    transaction.end();
  }

  @Test
  public void testQueryAsWithJoin() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");
    queryPlan.join("t2", "t1.int", "t2.int");

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.int");
    columnNames.add("t2.int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.execute();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());
      assertEquals(values.get(0), values.get(1));
    }

    transaction.end();
  }
}
