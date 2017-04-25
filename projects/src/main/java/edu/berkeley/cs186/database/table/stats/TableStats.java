package edu.berkeley.cs186.database.table.stats;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.io.Page;

/**
 * A wrapper class to represent the statistics for a single table.
 * Note that statistics are an estimate over the data in a table.
 * An instance of TableStats contains a list of Histograms that each
 * provide statistics about individual columns of a schema.
 *
 * YOU SHOULD NOT NEED TO CHANGE ANY OF THE CODE IN THIS CLASS.
 */
public class TableStats {
  private boolean estimate;
  private int numRecords;
  private int numPages;

  private Schema tableSchema;
  private List<Histogram> histograms;

  /**
   * Creates a new TableStats with a given Schema.
   *
   * @param tableSchema the schema instance associated with the target table
   */
  public TableStats(Schema tableSchema) {
    this.estimate = false;
    this.numRecords = 0;
    this.numPages = 0;

    this.tableSchema = tableSchema;
    this.histograms = new ArrayList<Histogram>();

    for (DataBox dataType : tableSchema.getFieldTypes()) {
      switch(dataType.type()) {
        case INT:
          this.histograms.add(new IntHistogram());
          break;
        case FLOAT:
          this.histograms.add(new FloatHistogram());
          break;
        case BOOL:
          this.histograms.add(new BoolHistogram());
          break;
        case STRING:
          this.histograms.add(new StringHistogram());
          break;
        default:
          break;
      }
    }
  }

  /**
   * Creates a new TableStats with a schema, a list of histograms,
   * and an estimate number of records.
   *
   * @param tableSchema the schema instance associated with the target table
   * @param histograms a list of histograms associated with the fields in tableSchema
   * @param numRecords the estimate number of records the target table contains
   */
  public TableStats(Schema tableSchema, List<Histogram> histograms, int numRecords) {
    this.estimate = true;
    this.numRecords = numRecords;

    this.tableSchema = tableSchema;
    this.histograms = histograms;

    this.numPages = this.calculateNumPages();
  }

  /**
   * Calculates the estimate number of pages the target table
   * contains - assumes that the table is densely packed.
   *
   * @return the estimate number of pages
   */
  private int calculateNumPages() {
    int pageHeaderSize = ((Page.pageSize * 8) / (1 + 8 * this.tableSchema.getEntrySize())) / 8;
    int numEntriesPerPage = pageHeaderSize * 8;
    int numPages = (this.numRecords + numEntriesPerPage - 1) / numEntriesPerPage;

    return numPages;
  }

  /**
   * Adds the stats for a new record.
   *
   * @param record the new record
   */
  public void addRecord(Record record) {
    this.numRecords++;
    this.numPages = this.calculateNumPages();

    int count = 0;
    for (DataBox value : record.getValues()) {
      switch(value.type()) {
        case INT:
          this.histograms.get(count).addValue(value.getInt());
          break;
        case STRING:
          this.histograms.get(count).addValue(value.getString());
          break;
        case BOOL:
          this.histograms.get(count).addValue(value.getBool());
          break;
        case FLOAT:
          this.histograms.get(count).addValue(value.getFloat());
          break;
        default:
          break;
      }

      count++;
    }
  }

  /**
   * Remove the stats for an existing record.
   *
   * @param record the new record
   */
  public void removeRecord(Record record) {
    this.numRecords--;
    this.numPages = this.calculateNumPages();

    int count = 0;
    for (DataBox value : record.getValues()) {
      switch(value.type()) {
        case INT:
          this.histograms.get(count).removeValue(value.getInt());
          break;
        case STRING:
          this.histograms.get(count).removeValue(value.getString());
          break;
        case BOOL:
          this.histograms.get(count).removeValue(value.getBool());
          break;
        case FLOAT:
          this.histograms.get(count).removeValue(value.getFloat());
          break;
        default:
          break;
      }

      count++;
    }
  }

  /**
   * Gets the number of records the target table contains.
   *
   * @return number of records
   */
  public int getNumRecords() { return this.numRecords; }

  /**
   * Gets the number of pages the target table contains.
   *
   * @return number of pages
   */
  public int getNumPages() { return this.numPages; }

  /**
   * Gets the estimate reduction factor a predicate
   * and value would result in over a certain column.
   *
   * @param index the index of column in question
   * @param predicate the predicate to compute reduction factor with
   * @param value the value to compare against
   * @return estimate reduction factor
   */
  public float getReductionFactor(int index,
                                  QueryPlan.PredicateOperator predicate,
                                  DataBox value) {
    return this.getHistogram(index).computeReductionFactor(predicate, value);
  }

  /**
   * Creates a new TableStats which is the statistics for the table
   * that results from the given predicate and value applied to
   * the target table associated with this TableStats.
   *
   * @param index the index of column in question
   * @param predicate the predicate to compute reduction factor with
   * @param value the value to compare against
   * @return new TableStats based off of this and params
   */
  public TableStats copyWithPredicate(int index,
                                      QueryPlan.PredicateOperator predicate,
                                      DataBox value) {
    List<Histogram> copyHistograms = new ArrayList<Histogram>();

    Histogram predHistogram = this.histograms.get(index);
    float reductionFactor = predHistogram.computeReductionFactor(predicate, value);

    for (int i = 0; i < this.histograms.size(); i++) {
      if (i == index) {
        copyHistograms.add(predHistogram.copyWithPredicate(predicate, value));
      } else {
        Histogram histogram = this.histograms.get(i);
        copyHistograms.add(histogram.copyWithReduction(reductionFactor));
      }
    }

    int numRecords = (int) (this.numRecords * reductionFactor);
    return new TableStats(this.tableSchema, copyHistograms, numRecords);
  }

  /**
   * Creates a new TableStats which is the statistics for the table
   * that results from this TableStats joined with the given TableStats.
   *
   * @param leftIndex the index of the join column for this
   * @param rightStats the TableStats of the right table to be joined
   * @param leftIndex the index of the join column for the right table
   * @return new TableStats based off of this and params
   */
  public TableStats copyWithJoin(int leftIndex,
                                 TableStats rightStats,
                                 int rightIndex) {
    // Assume `this` is the `TableStats` instance for the left relation.
    Schema rightSchema = rightStats.getSchema();
    List<Histogram> rightHistograms = rightStats.getHistograms();

    List<String> rightFieldNames = rightSchema.getFieldNames();
    List<DataBox> rightDataBoxs = rightSchema.getFieldTypes();

    List<String> copyFields = new ArrayList<String>();

    for (String leftFieldName : this.tableSchema.getFieldNames()) {
      copyFields.add(leftFieldName);
    }

    for (String rightFieldName : rightFieldNames) {
      copyFields.add(rightFieldName);
    }

    List<DataBox> copyDataBoxs = new ArrayList<DataBox>();

    for (DataBox leftDataBox : this.tableSchema.getFieldTypes()) {
      copyDataBoxs.add(leftDataBox);
    }

    for (DataBox rightDataBox : rightDataBoxs) {
      copyDataBoxs.add(rightDataBox);
    }

    Schema copySchema = new Schema(copyFields, copyDataBoxs);

    int inputSize = this.numRecords * rightStats.getNumRecords();
    int leftNumDistinct = this.getNumDistinct(leftIndex);
    int rightNumDistinct = rightStats.getNumDistinct(rightIndex);
    float reductionFactor = 1.0f / Math.max(leftNumDistinct, rightNumDistinct);

    List<Histogram> copyHistograms = new ArrayList<Histogram>();

    int leftNumRecords = this.numRecords;
    int rightNumRecords = rightStats.getNumRecords();

    float leftReductionFactor = ((float) inputSize / leftNumRecords) * reductionFactor;
    float rightReductionFactor = ((float) inputSize / rightNumRecords) * reductionFactor;

    float joinReductionFactor = leftReductionFactor;
    Histogram joinHistogram = this.histograms.get(leftIndex);

    for (int i = 0; i < this.histograms.size(); i++) {
      Histogram leftHistogram = this.histograms.get(i);
      if (i == leftIndex) {
        copyHistograms.add(joinHistogram.copyWithReduction(joinReductionFactor));
      } else {
        copyHistograms.add(leftHistogram.copyWithReduction(leftReductionFactor));
      }
    }

    for (int i = 0; i < rightHistograms.size(); i++) {
      Histogram rightHistogram = rightHistograms.get(i);
      if (i == rightIndex) {
        copyHistograms.add(joinHistogram.copyWithReduction(joinReductionFactor));
      } else {
        copyHistograms.add(rightHistogram.copyWithReduction(rightReductionFactor));
      }
    }

    int outputSize = (int) (inputSize * reductionFactor);
    return new TableStats(copySchema, copyHistograms, outputSize);
  }

  /**
   * Gets the histogram for a certain column.
   *
   * @param index the index of column in question
   * @return the histogram corresponding to index
   */
  public Histogram getHistogram(int index) {
    return this.histograms.get(index);
  }

  /**
   * Gets the histograms of this TableStatistics.
   *
   * @return list of histograms
   */
  public List<Histogram> getHistograms() {
    return this.histograms;
  }

  /**
   * Gets the schema of this TableStatistics.
   *
   * @return schema of this TableStats
   */
  public Schema getSchema() {
    return this.tableSchema;
  }

  /**
   * Gets the estimate number of distinct entries for a certain column.
   *
   * @param index the index of column in question
   * @return estimate number of distinct entries
   */
  public int getNumDistinct(int index) {
    return this.getHistogram(index).getNumDistinct();
  }
}