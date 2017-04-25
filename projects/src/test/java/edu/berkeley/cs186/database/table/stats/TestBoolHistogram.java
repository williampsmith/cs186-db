package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import static org.junit.Assert.*;

import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

public class TestBoolHistogram {

  @Test(timeout=1000)
  public void testBoolHistogram() {
    BoolHistogram histogram = new BoolHistogram();

    for (int i = 0; i < 100; i++) {
      histogram.addValue(true);
      histogram.addValue(false);
    }

    assertEquals(100, histogram.getEntriesInRange(false, false));
    assertEquals(100, histogram.getEntriesInRange(true, true));
    assertEquals(200, histogram.getEntriesInRange(false, true));

    assertEquals(2, histogram.getNumDistinct());
  }

  @Test(timeout=1000)
  public void testBoolComputeReductionFactor() {
    BoolHistogram histogram = new BoolHistogram();

    for (int i = 0; i < 30; i++) {
      histogram.addValue(true);
      histogram.addValue(false);
    }

    for (int i = 0; i < 40; i++) {
      histogram.addValue(true);
    }

    assertEquals(2, histogram.getNumDistinct());

    BoolDataBox equalsValue = new BoolDataBox(true);
    assertEquals(0.7f,
                 histogram.computeReductionFactor(PredicateOperator.EQUALS,
                                                  equalsValue),
                 0.001f);

    BoolDataBox notEqualsValue = new BoolDataBox(true);
    assertEquals(0.3f,
                 histogram.computeReductionFactor(PredicateOperator.NOT_EQUALS,
                                                  notEqualsValue),
                 0.001f);
  }

  @Test(timeout=1000)
  public void testBoolCopyWithReduction() {
    BoolHistogram histogram = new BoolHistogram();

    for (int i = 0; i < 100; i++) {
      histogram.addValue(true);
      histogram.addValue(false);
    }

    BoolHistogram copyHistogram = histogram.copyWithReduction(0.7f);

    assertEquals(70, copyHistogram.getEntriesInRange(false, false));
    assertEquals(70, copyHistogram.getEntriesInRange(true, true));
    assertEquals(140, copyHistogram.getEntriesInRange(false, true));

    assertEquals(2, copyHistogram.getNumDistinct());
  }

  @Test(timeout=1000)
  public void testBoolCopyWithPredicate() {
    BoolHistogram histogram = new BoolHistogram();

    for (int i = 0; i < 100; i++) {
      histogram.addValue(true);
      histogram.addValue(false);
    }

    BoolDataBox value = new BoolDataBox(true);
    BoolHistogram copyHistogram = histogram.copyWithPredicate(PredicateOperator.EQUALS, value);

    assertEquals(100, copyHistogram.getEntriesInRange(true, true));
    assertEquals(100, copyHistogram.getEntriesInRange(false, true));

    assertEquals(1, copyHistogram.getNumDistinct());
  }
}