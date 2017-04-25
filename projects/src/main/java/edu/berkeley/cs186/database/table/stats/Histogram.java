package edu.berkeley.cs186.database.table.stats;

import java.util.List;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

/**
 * A parametrized type that stores histograms for a given value.
 *
 * @param <T> the type of the histogram
 */
public interface Histogram<T> {

  /**
   * Add a new value to the Histogram.
   *
   * @param value the value to add
   */
  void addValue(T value);

  /**
   * Removes a value from the Histogram
   *
   * @param value the value to remove
   */
  void removeValue(T value);

  float computeReductionFactor(PredicateOperator predicate,
                               DataBox value);
  /**
   * Get the number of values within a given range, including start and up to but not including end.
   *
   * @param start the inclusive start of the range
   * @param end the non-inclusive end of the range
   * @return the number of values in this range
   */
  int getEntriesInRange(T start, T end);

  int getMinValue();

  int getMaxValue();

  int getNumDistinct();

  Histogram<T> copyWithReduction(float reductionFactor);

  Histogram<T> copyWithPredicate(PredicateOperator predicate,
                                 DataBox value);
}