package edu.berkeley.cs186.database.table.stats;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

public class FloatHistogram implements Histogram<Float> {
  private static int mulFactor = 10000;

  private IntHistogram histogram;

  public FloatHistogram() {
    this.histogram = new IntHistogram();
  }

  public FloatHistogram(IntHistogram copyHistogram) {
    this.histogram = copyHistogram;
  }

  public FloatHistogram copyWithReduction(float reductionFactor) {
    IntHistogram copyHistogram = this.histogram.copyWithReduction(reductionFactor);
    return new FloatHistogram(copyHistogram);
  }

  public FloatHistogram copyWithPredicate(PredicateOperator predicate,
                                          DataBox value) {
    IntHistogram copyHistogram = this.histogram.copyWithPredicate(predicate, value);
    return new FloatHistogram(copyHistogram);
  }

  public List<Bucket<Integer>> getAllBuckets() {
    return this.histogram.getAllBuckets();
  }

  public void addValue(Float value) {
    // Converts given float into an int - only four digits
    // past the decimal point will be kept. Note this assumes
    // value * mulFactor will not exceed the maximum int.
    int intValue = (int) (value * mulFactor);
    this.histogram.addValue(intValue);
  }

  public void removeValue(Float value) {
    int intValue = (int) (value * mulFactor);
    this.histogram.removeValue(intValue);
  }

  public float computeReductionFactor(PredicateOperator predicate,
                                      DataBox value) {
    return this.histogram.computeReductionFactor(predicate, value);
  }

  public int getEntriesInRange(Float start, Float end) {
    int intStart = (int) (start * mulFactor);
    int intEnd = (int) (end * mulFactor);
    return this.histogram.getEntriesInRange(intStart, intEnd);
  }

  public int getMinValue() {
    return this.histogram.getMinValue();
  }

  public int getMaxValue() {
    return this.histogram.getMaxValue();
  }

  public int getNumDistinct() {
    return this.histogram.getNumDistinct();
  }
}