package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IndexScanOperator extends QueryOperator {
  private Database.Transaction transaction;
  private String tableName;
  private String columnName;
  private QueryPlan.PredicateOperator predicate;
  private DataBox value;

  private int columnIndex;

  /**
   * An index scan operator.
   *
   * @param transaction the transaction containing this operator
   * @param tableName the table to iterate over
   * @param columnName the name of the column the index is on
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public IndexScanOperator(Database.Transaction transaction,
                           String tableName,
                           String columnName,
                           QueryPlan.PredicateOperator predicate,
                           DataBox value) throws QueryPlanException, DatabaseException {
    super(OperatorType.INDEXSCAN);
    this.tableName = tableName;
    this.transaction = transaction;
    this.columnName = columnName;
    this.predicate = predicate;
    this.value = value;
    this.setOutputSchema(this.computeSchema());
    columnName = this.checkSchemaForColumn(this.getOutputSchema(), columnName);
    this.columnIndex = this.getOutputSchema().getFieldNames().indexOf(columnName);
  }

  public String toString() {
    return "type: " + this.getType() +
        "\ntable: " + this.tableName +
        "\ncolumn: " + this.columnName +
        "\noperator: " + this.predicate +
        "\nvalue: " + this.value;
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new IndexScanIterator();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class IndexScanIterator implements Iterator<Record> {
    /* TODO: Implement the IndexScanIterator */
    private Iterator<Record> recordIterator;
    private boolean done;
    private boolean implemented;
    private Record nextRecord;

    public IndexScanIterator() throws QueryPlanException, DatabaseException {
      IndexScanOperator iso = IndexScanOperator.this;
      // TODO: test this
      if (!iso.transaction.indexExists(iso.tableName, iso.columnName)) {
        this.done = true;
        throw new QueryPlanException("Error. Index does not exist for column " + iso.columnName +
                                     "of table " + iso.tableName);
      }

      if (iso.predicate == QueryPlan.PredicateOperator.EQUALS) {
        recordIterator = iso.transaction.lookupKey(iso.tableName, iso.columnName, iso.value);
        this.implemented = true;
      } else if (iso.predicate == QueryPlan.PredicateOperator.GREATER_THAN_EQUALS) {
        recordIterator = iso.transaction.sortedScan(iso.tableName, iso.columnName);
        this.implemented = true;
      } else if (iso.predicate == QueryPlan.PredicateOperator.GREATER_THAN) {
        this.recordIterator = null;
        this.implemented = false;
      } else if (iso.predicate == QueryPlan.PredicateOperator.LESS_THAN) {
        this.recordIterator = null;
        this.implemented = false;
      } else if (iso.predicate == QueryPlan.PredicateOperator.LESS_THAN_EQUALS) {
        this.recordIterator = null;
        this.implemented = false;
      } else if (iso.predicate == QueryPlan.PredicateOperator.NOT_EQUALS) {
        this.recordIterator = null;
        this.implemented = false;
      } else {
        System.out.println("Error: Did not recognize index scan predicate.");
      }

      this.done = false;
      this.nextRecord = null;
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      /* TODO */
      IndexScanOperator iso = IndexScanOperator.this;
      if (this.implemented) {
        return this.recordIterator.hasNext();
      }

      // TODO check this ordering
      if (this.nextRecord != null) {
        return true;
      }

      // TODO check this ordering
      if (this.done) {
        return false;
      }

      while (true) {
        if (!this.recordIterator.hasNext()) {
          done = true;
          return false;
        }
        this.nextRecord = this.recordIterator.next();
        int comparison = this.compareRecord(this.nextRecord);

        if (iso.predicate == QueryPlan.PredicateOperator.GREATER_THAN) {
          if (comparison == 1) {
            return true;
          }
          // otherwise, go to next record
          this.nextRecord = null;
          continue;
        }

        if (iso.predicate == QueryPlan.PredicateOperator.LESS_THAN) {
          if (comparison == -1) {
            return true;
          }
          // otherwise, go to next record
          this.nextRecord = null;
          this.done = true;
          continue;
        }

        if (iso.predicate == QueryPlan.PredicateOperator.LESS_THAN_EQUALS) {
          if (comparison == -1 || comparison == 0) {
            return true;
          }
          // otherwise, go to next record
          this.nextRecord = null;
          this.done = true;
          continue;
        }

        if (iso.predicate == QueryPlan.PredicateOperator.NOT_EQUALS) {
          if (comparison == -1 || comparison == 1) {
            return true;
          }
          // otherwise, go to next record
          this.nextRecord = null;
          continue;
        }
      }
    }

    public int compareRecord(Record record) {
      IndexScanOperator iso = IndexScanOperator.this;
      return record.getValues().get(iso.columnIndex).compareTo(iso.value);
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.implemented) {
        return this.recordIterator.next();
      }
      else {
        if (this.hasNext()) {
          Record r = this.nextRecord;
          this.nextRecord = null;
          return r;
        }
        throw new NoSuchElementException();
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
