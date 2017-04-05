package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;
import org.omg.CORBA.INTERNAL;

import java.lang.reflect.Array;
import java.util.*;
import java.lang.*;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator implements Iterator<Record> {
    /* TODO: Implement the SortMergeIterator */

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
    }

    /**
    * Checks if there are more record(s) to yield
    *
    * @return true if this iterator has another record to yield, otherwise false
    */
    public boolean hasNext() {
      /* TODO */
      return false;
    }

    private boolean advanceLeftTable() {
      /* TODO */
      return false;
    }

    private boolean advanceRightTable() {
      /* TODO */
      return false;
    }

    private Record getNextLeftRecordInPage() {
      /* TODO */
      return null;
    }

    private Record getNextRightRecordInPage() {
      /* TODO */
      return null;
    }

    /**
    * Yields the next record of this iterator.
    *
    * @return the next Record
    * @throws NoSuchElementException if there are no more Records to yield
    */
    public Record next() {
      /* TODO */
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
