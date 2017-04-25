package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.StudentTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

public class TestFloatHistogram {

  @Test
  public void testFloatSimpleHistogram() {
    FloatHistogram histogram = new FloatHistogram();

    for (int i = 0; i < 10; i++) {
      histogram.addValue(i * 1.0f);
    }

    assertEquals(10, histogram.getEntriesInRange(0.0f, 10.0f));
  }

  @Test
  public void testFloatComplexHistogram() {
    FloatHistogram histogram = new FloatHistogram();

    for (int i = 0; i < 40; i++) {
      histogram.addValue(i * 1.0f);
    }

    assertEquals(11, histogram.getEntriesInRange(0.0f, 10.0f));
  }
}
