package edu.berkeley.cs186.database.query;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP4;
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

public class TestOptimalQueryPlanJoins {
  private Database database;
  private Random random = new Random();
  private String alphabet = StringHistogram.alphaNumeric;
  private String defaulTableName = "testAllTypes";
  private int defaultNumRecords = 1000;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("db");
    this.database = new Database(tempDir.getAbsolutePath());
    this.database.deleteAllTables();
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), this.defaulTableName);
    Database.Transaction transaction = this.database.beginTransaction();

    // by default, create 100 records
    for (int i = 0; i < this.defaultNumRecords; i++) {
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

  @Test(timeout=5000)
  public void testSimpleJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");
    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.int", "t2.int");
    Iterator<Record> outputIterator = queryPlan.executeOptimal();

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

    assertTrue(count == this.defaultNumRecords);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: BNLJ\n" +
                  "leftColumn: t2.int\n" +
                  "rightColumn: t1.int\n" +
                  "\t(left)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t1";
    String tree2 = "type: BNLJ\n" +
                  "leftColumn: t1.int\n" +
                  "rightColumn: t2.int\n" +
                  "\t(left)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t1\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2";
    assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));

    transaction.end();
  }

  @Test(timeout=5000)
  public void testProjectJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.int");
    columnNames.add("t2.string");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    int count = 0;
    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof IntDataBox);
      assertTrue(values.get(1) instanceof StringDataBox);

      count++;
    }

    // We test `>=` instead of `==` since strings are generated
    // randomly and there's a small chance of duplicates.
    assertTrue(count >= 1000);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: PROJECT\n" +
                  "columns: [t1.int, t2.string]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t2.string\n" +
                  "\trightColumn: t1.string\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1";
    String tree2 = "type: PROJECT\n" +
                  "columns: [t1.int, t2.string]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t1.string\n" +
                  "\trightColumn: t2.string\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2";
    assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));

    transaction.end();
  }

  @Test(timeout=5000)
  public void testSelectJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(values.get(0), values.get(4));
      assertEquals(values.get(1), values.get(5));
      assertEquals(values.get(2), values.get(6));
      assertEquals(values.get(3), values.get(7));

      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: BNLJ\n" +
                  "leftColumn: t1.string\n" +
                  "rightColumn: t2.string\n" +
                  "\t(left)\n" +
                  "\ttype: SELECT\n" +
                  "\tcolumn: t1.bool\n" +
                  "\toperator: NOT_EQUALS\n" +
                  "\tvalue: false\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=5000)
  public void testProjectSelectJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add("t2.int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);

      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: PROJECT\n" +
                  "columns: [t1.bool, t2.int]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t1.string\n" +
                  "\trightColumn: t2.string\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: SELECT\n" +
                  "\t\tcolumn: t1.bool\n" +
                  "\t\toperator: NOT_EQUALS\n" +
                  "\t\tvalue: false\n" +
                  "\t\t\ttype: SEQSCAN\n" +
                  "\t\t\ttable: t1\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }
}
