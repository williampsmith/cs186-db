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
  
  public int estimateIOCost() throws QueryPlanException {
    // You don't need to implement this.
    throw new QueryPlanException("Not yet implemented!");
  }

  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator implements Iterator<Record> {
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int leftEntryNum;
    private int rightEntryNum;
    private Page[] block;
    private int rightEntryMark;
    private boolean marked;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      if (SortMergeOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator)SortMergeOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getLeftColumnName() + "Left";
        SortMergeOperator.this.createTempTable(SortMergeOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = SortMergeOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          SortMergeOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (SortMergeOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator)SortMergeOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getRightColumnName() + "Right";
        SortMergeOperator.this.createTempTable(SortMergeOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = SortMergeOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          SortMergeOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      this.leftTableName = sortMerge(this.leftTableName, new LeftRecordComparator());
      this.rightTableName = sortMerge(this.rightTableName, new RightRecordComparator());

      this.leftIterator = SortMergeOperator.this.getPageIterator(this.leftTableName);
      this.rightIterator = SortMergeOperator.this.getPageIterator(this.rightTableName);
      this.nextRecord = null;
      this.leftEntryNum = 0;
      this.rightEntryNum = 0;
      this.marked = false;
      if (this.leftIterator.hasNext()) {
        assert (this.leftIterator.next().getPageNum() == 0);
        if (this.leftIterator.hasNext()) {
          this.leftPage = this.leftIterator.next();
          this.leftHeader = SortMergeOperator.this.getPageHeader(this.leftTableName, this.leftPage);
          advanceLeftTable();
        }
      }
      if (this.rightIterator.hasNext()) {
        assert(this.rightIterator.next().getPageNum() == 0);
        if (this.rightIterator.hasNext()) {
          this.rightPage = this.rightIterator.next();
          this.rightHeader = SortMergeOperator.this.getPageHeader(this.rightTableName, this.rightPage);
          advanceRightTable();
        }
      }
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }
      if (this.leftRecord == null || this.leftPage == null || this.rightPage == null) {
        return false;
      }
      while (true) {
        if (this.rightRecord != null) {
          DataBox leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
          while (leftJoinValue.compareTo(rightJoinValue) < 0) {
            if (marked) {
              this.rightEntryNum = this.rightEntryMark;
              advanceRightTable();
              rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
            }
            marked = false;
            if (!advanceLeftTable()) {
              return false;
            }
            leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
          }
          while (leftJoinValue.compareTo(rightJoinValue) > 0) {
            if (marked) {
              this.rightEntryNum = this.rightEntryMark;
              advanceRightTable();
            }
            marked = false;
            if (!advanceRightTable()) {
              return false;
            }
            rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
          }
          if (leftJoinValue.compareTo(rightJoinValue) == 0 && !marked) {
            marked = true;
            this.rightEntryMark = this.rightEntryNum-1;
          }
          while (leftJoinValue.compareTo(rightJoinValue) == 0) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            advanceRightTable();
            return true;
          }
        } else {
          if (!advanceRightTable()) {
            if (advanceLeftTable()){
              this.rightEntryNum = this.rightEntryMark;
              advanceRightTable();
            } else {
              return false;
            }
          }
        }
      }

    }

    private String sortMerge(String tableName, Comparator<Record> comparator) throws DatabaseException {
      Iterator<Record> iter = SortMergeOperator.this.getTableIterator(tableName);
      List<Record> records = new ArrayList<Record>();
      while (iter.hasNext()) {
        records.add(iter.next());
      }
      Collections.sort(records, comparator);
      Schema s = SortMergeOperator.this.getSchema(tableName);
      SortMergeOperator.this.createTempTable(s, tableName+"sorted");
      Iterator<Record> sortedIter = records.iterator();
      while (sortedIter.hasNext()) {
        SortMergeOperator.this.addRecord(tableName+"sorted", sortedIter.next().getValues());
      }
      return tableName+"sorted";
    }

    private boolean advanceLeftTable() {
      this.leftRecord = getNextLeftRecordInPage();
      if (this.leftRecord == null) {
        while (this.leftIterator.hasNext()) {
          try {
            this.leftPage = this.leftIterator.next();
            this.leftHeader = SortMergeOperator.this.getPageHeader(this.leftTableName, this.leftPage);
            this.leftEntryNum = 0;
            this.leftRecord = getNextLeftRecordInPage();
            if (this.leftRecord != null) {
              return true;
            }
          } catch (DatabaseException e) {
            return false;
          }
        }
        return false;
      } else {
        return true;
      }
    }

    private boolean advanceRightTable() {
      this.rightRecord = getNextRightRecordInPage();
      if (this.rightRecord == null) {
        while (this.rightIterator.hasNext()) {
          try {
            this.rightPage = this.rightIterator.next();
            this.rightHeader = SortMergeOperator.this.getPageHeader(this.rightTableName, this.rightPage);
            this.rightEntryNum = 0;
            this.rightRecord = getNextRightRecordInPage();
            if (this.rightRecord != null) {
              return true;
            }
          } catch (DatabaseException e) {
            return false;
          }
        }
        return false;
      } else {
        return true;
      }
    }

    private Record getNextLeftRecordInPage() {
      try {
        while (this.leftEntryNum < SortMergeOperator.this.getNumEntriesPerPage(this.leftTableName)) {
          byte b = leftHeader[this.leftEntryNum / 8];
          int bitOffset = 7 - (this.leftEntryNum % 8);
          byte mask = (byte) (1 << bitOffset);
          byte value = (byte) (b & mask);
          if (value != 0) {
            int entrySize = SortMergeOperator.this .getEntrySize(this.leftTableName);
            int offset = SortMergeOperator.this.getHeaderSize(this.leftTableName) + (entrySize * this.leftEntryNum);
            byte[] bytes = this.leftPage.readBytes(offset, entrySize);
            Record toRtn = SortMergeOperator.this.getLeftSource().getOutputSchema().decode(bytes);
            this.leftEntryNum++;
            return toRtn;
          }
          this.leftEntryNum++;
        }
      } catch (DatabaseException d)  {
        return null;
      }
      return null;
    }

    private Record getNextRightRecordInPage() {
      try {
        while (this.rightEntryNum < SortMergeOperator.this.getNumEntriesPerPage(this.rightTableName)) {
          byte b = rightHeader[this.rightEntryNum / 8];
          int bitOffset = 7 - (this.rightEntryNum % 8);
          byte mask = (byte) (1 << bitOffset);
          byte value = (byte) (b & mask);
          if (value != 0) {
            int entrySize = SortMergeOperator.this.getEntrySize(this.rightTableName);
            int offset = SortMergeOperator.this.getHeaderSize(this.rightTableName) + (entrySize * rightEntryNum);
            byte[] bytes = this.rightPage.readBytes(offset, entrySize);
            Record toRtn = SortMergeOperator.this.getRightSource().getOutputSchema().decode(bytes);
            this.rightEntryNum++;
            return toRtn;
          }
          this.rightEntryNum++;
        }
      } catch (DatabaseException d) {
        return null;
      }
      return null;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.hasNext()) {
        Record r = this.nextRecord;
        this.nextRecord = null;
        return r;
      }
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