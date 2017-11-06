package edu.berkeley.cs186.database.table.stats;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

public class BoolHistogram implements Histogram<Boolean> {
  private boolean estimate;
  private int numDistinct;

  private List<Bucket<Boolean>> buckets;

  public BoolHistogram() {
    this.estimate = false;

    this.buckets = new ArrayList<Bucket<Boolean>>();
    this.buckets.add(new Bucket<Boolean>(true));
    this.buckets.add(new Bucket<Boolean>(false));
  }

  public BoolHistogram(List<Bucket<Boolean>> buckets, int numDistinct) {
    this.numDistinct = numDistinct;
    this.estimate = true;

    this.buckets = buckets;
  }

  public BoolHistogram copyWithReduction(float reductionFactor) {
    List<Bucket<Boolean>> copyBuckets = new ArrayList<Bucket<Boolean>>();

    int trueCount = (int) (this.buckets.get(0).getCount() * reductionFactor);
    int falseCount = (int) (this.buckets.get(1).getCount() * reductionFactor);

    copyBuckets.add(new Bucket<Boolean>(true));
    copyBuckets.get(0).increment(trueCount);
    copyBuckets.add(new Bucket<Boolean>(false));
    copyBuckets.get(1).increment(falseCount);

    return new BoolHistogram(copyBuckets, this.getNumDistinct());
  }

  public BoolHistogram copyWithPredicate(PredicateOperator predicate,
                                         DataBox value) {
    List<Bucket<Boolean>> copyBuckets = new ArrayList<Bucket<Boolean>>();

    copyBuckets.add(new Bucket<Boolean>(true));
    copyBuckets.add(new Bucket<Boolean>(false));

    int trueCount = this.buckets.get(0).getCount();
    int falseCount = this.buckets.get(1).getCount();
    boolean predValue = value.getBool();

    switch (predicate) {
      case EQUALS:
        if (predValue == true) {
          copyBuckets.get(0).increment(trueCount);
        } else {
          copyBuckets.get(1).increment(falseCount);
        }
        break;
      case NOT_EQUALS:
        if (predValue == true) {
          copyBuckets.get(1).increment(falseCount);
        } else {
          copyBuckets.get(0).increment(trueCount);
        }
        break;
      default:
        break;
    }

    int numDistinct = 0;

    if (copyBuckets.get(0).getCount() > 0) {
      numDistinct += 1;
    }
    if (copyBuckets.get(1).getCount() > 0) {
      numDistinct += 1;
    }

    return new BoolHistogram(copyBuckets, numDistinct);
  }

  public float computeReductionFactor(PredicateOperator predicate,
                                      DataBox value) {
    float reductionFactor = 1;

    boolean predValue = value.getBool();
    int trueCount = this.buckets.get(0).getCount();
    int falseCount = this.buckets.get(1).getCount();

    float totalCount = trueCount + falseCount;

    switch (predicate) {
      case EQUALS:
        if (predValue == true) {
          reductionFactor = trueCount / totalCount;
        } else {
          reductionFactor = falseCount / totalCount;
        }
        break;
      case NOT_EQUALS:
        if (predValue == true) {
          reductionFactor = falseCount / totalCount;
        } else {
          reductionFactor = trueCount / totalCount;
        }
        break;
      default:
        break;
    }

    return reductionFactor;
  }

  public List<Bucket<Boolean>> getAllBuckets() {
    return this.buckets;
  }

  public void removeValue(Boolean value) {
    if (value) {
      this.buckets.get(0).decrement();
    } else {
      this.buckets.get(1).decrement();
    }
  }

  public void addValue(Boolean value) {
    if (value) {
      this.buckets.get(0).increment();
    } else {
      this.buckets.get(1).increment();
    }
  }

  public int getEntriesInRange(Boolean start, Boolean end) {
    int entries = 0;

    if (start) {
      entries = this.buckets.get(0).getCount();
    } else {
      if (end) {
        entries += this.buckets.get(0).getCount();
      }
      entries += this.buckets.get(1).getCount();
    }

    return entries;
  }

  public int getMinValue() {
    throw new UnsupportedOperationException();
  }

  public int getMaxValue() {
    throw new UnsupportedOperationException();
  }

  public int getNumDistinct() {
    int numDistinct = 0;

    if (this.buckets.get(0).getCount() > 0) {
      numDistinct += 1;
    }
    if (this.buckets.get(1).getCount() > 0) {
      numDistinct += 1;
    }

    return numDistinct;
  }
}