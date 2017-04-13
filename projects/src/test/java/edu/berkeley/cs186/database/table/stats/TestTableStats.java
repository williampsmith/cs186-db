package edu.berkeley.cs186.database.table.stats;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;
import edu.berkeley.cs186.database.table.Schema;
import static org.junit.Assert.*;

import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

public class TestTableStats {

  @Test(timeout=1000)
  public void testTableStats() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    TableStats stats = new TableStats(schema);

    for (int i = 0; i < 100; i++) {
      stats.addRecord(TestUtils.createRecordWithAllTypes());
    }

    assertEquals(100, stats.getNumRecords());

    Histogram histOne = stats.getHistogram(0);
    assertTrue(histOne instanceof BoolHistogram);
    assertEquals(100, histOne.getEntriesInRange(true, null));

    Histogram histTwo = stats.getHistogram(1);
    assertTrue(histTwo instanceof IntHistogram);
    assertEquals(50, histTwo.getEntriesInRange(0, 5));

    Histogram histThree = stats.getHistogram(2);
    assertTrue(histThree instanceof StringHistogram);
    assertEquals(100, histThree.getEntriesInRange("a", "b"));

    Histogram histFour = stats.getHistogram(3);
    assertTrue(histFour instanceof FloatHistogram);
    assertEquals(100, histFour.getEntriesInRange(0.0f, 5.0f));
  }

  @Test(timeout=1000)
  public void testTableStatsCopyWithPredicate() {
    List<DataBox> dataTypes = new ArrayList<DataBox>();
    List<String> fieldNames = new ArrayList<String>();

    dataTypes.add(new IntDataBox());
    dataTypes.add(new BoolDataBox());
    dataTypes.add(new StringDataBox());

    fieldNames.add("int");
    fieldNames.add("bool");
    fieldNames.add("string");

    Schema schema = new Schema(fieldNames, dataTypes);
    TableStats stats = new TableStats(schema);

    for (int i = 0; i < 100; i++) {
      List<DataBox> values = new ArrayList<DataBox>();
      values.add(new IntDataBox(i));

      if (i < 50) {
        values.add(new BoolDataBox(true));
      } else {
        values.add(new BoolDataBox(false));
      }

      values.add(new StringDataBox("cs186", 5));

      stats.addRecord(new Record(values));
    }

    DataBox value = new IntDataBox(50);

    float reductionFactor = stats.getReductionFactor(
      0, PredicateOperator.LESS_THAN, value);
    assertEquals(0.5f, reductionFactor, 0.01f);

    TableStats copyStats = stats.copyWithPredicate(
      0, PredicateOperator.LESS_THAN, value);
    assertEquals(50, copyStats.getNumRecords());

    IntHistogram copyIntHistogram = (IntHistogram) copyStats.getHistogram(0);
    assertEquals(50, copyIntHistogram.getNumDistinct());
    assertEquals(50, copyIntHistogram.getEntriesInRange(0, 100));

    BoolHistogram copyBoolHistogram = (BoolHistogram) copyStats.getHistogram(1);
    assertEquals(2, copyBoolHistogram.getNumDistinct());
    assertEquals(25, copyBoolHistogram.getEntriesInRange(false, false));
    assertEquals(25, copyBoolHistogram.getEntriesInRange(true, true));

    StringHistogram copyStringHistogram = (StringHistogram) copyStats.getHistogram(2);
    assertEquals(1, copyStringHistogram.getNumDistinct());
    assertEquals(50, copyStringHistogram.getEntriesInRange("a", "9"));
  }
}