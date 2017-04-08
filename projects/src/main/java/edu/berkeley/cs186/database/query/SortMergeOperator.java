package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.DataBoxException;
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
    /* DONE */
    String leftTableName;
    String rightTableName;
    Iterator<Record> leftSortedTableIterator;
    Iterator<Page> rightPageIterator;
    Record leftRecord;
    Record rightRecord;
    Record nextRecord;
    int leftEntryNum;
    int rightEntryNum;
    Page rightPage;
    byte[] rightHeader;
    int blockMarker;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      /* DONE */
      this.leftTableName = "Left Sorted Table 1";
      this.rightTableName = "Right Sorted Table 1";
      Iterator<Record> leftIterator = getLeftSource().iterator();
      Iterator<Record> rightIterator = getRightSource().iterator();
      ArrayList<Record> leftTableRecords = new ArrayList<Record>();
      ArrayList<Record> rightTableRecords = new ArrayList<Record>();

      // materialize whole left table, sort, and write to sorted table
      while (leftIterator.hasNext()) {
        leftTableRecords.add(leftIterator.next());
      }
      Collections.sort(leftTableRecords, new LeftRecordComparator());

      SortMergeOperator.this.createTempTable(getLeftSource().getOutputSchema(), this.leftTableName);
      for (Record record : leftTableRecords) {
        addRecord(this.leftTableName, record.getValues());
      }

      // materialize whole right table, sort, and write to sorted table
      while (rightIterator.hasNext()) {
        rightTableRecords.add(rightIterator.next());
      }
      Collections.sort(rightTableRecords, new RightRecordComparator());

      SortMergeOperator.this.createTempTable(getRightSource().getOutputSchema(), this.rightTableName);
      for (Record record : rightTableRecords) {
        addRecord(this.rightTableName, record.getValues());
      }

      this.leftSortedTableIterator = SortMergeOperator.this.getTableIterator(leftTableName);
      this.rightPageIterator = SortMergeOperator.this.getPageIterator(this.rightTableName);
      this.rightPageIterator.next(); // discard header page
      this.leftRecord = null;
      this.rightRecord = null;
      this.nextRecord = null;
      this.leftEntryNum = 0;
      this.rightEntryNum = 0;
      this.rightPage = null;
      this.rightHeader = null;
      this.blockMarker = -1; // invalid, needs to be set upon equality
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

      while (true) {
        /* handle getting next left record */
        if (this.leftRecord == null) {
          if (!this.leftSortedTableIterator.hasNext()) {
            return false;
          }
          this.leftRecord = this.leftSortedTableIterator.next();
        }

        /* handle getting next page */
        if (this.rightPage == null) {
          if (!this.rightPageIterator.hasNext()) {
            return false;
          }
          this.rightPage = this.rightPageIterator.next();
          try {
            this.rightHeader = SortMergeOperator.this.getPageHeader(this.rightTableName, this.rightPage);
          } catch (DatabaseException e) {
            System.out.println("Error retrieving page header for page: " + this.rightPage.toString());
            return false;
          }
          this.rightEntryNum = 0;
        }

        /* handle getting next right record */
        if (this.rightRecord == null) {
          this.rightRecord = this.getNextRightRecordInPage();
          if (this.rightRecord == null) { // reached end of page
            this.rightPage = null;
            continue;
          }
        }

        /* merge phase */
        int comparison = this.mergeCompare();
        if (comparison < 0) { // advance left
          this.leftRecord = null;
          continue;
        } else if (comparison > 0) { // advance right
          this.rightRecord = null;
          continue;
        }

        if (this.blockMarker == -1) { // needs to be set to entryNum as beginning of block
          this.blockMarker = this.rightEntryNum;
        }

        /* yield record on equality */
        List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<DataBox>(this.rightRecord.getValues());
        leftValues.addAll(rightValues);
        this.nextRecord = new Record(leftValues);
        if (this.advanceRightTable()) { // if we reach end of equality block, invalidate left record
          this.rightRecord = this.getNextRightRecordInPage();
          if (this.rightRecord == null) { // reached end of page
            this.rightPage = null;
            this.rightEntryNum = 0;
          }
        } else { // invalidate the block
          this.rightEntryNum = this.blockMarker;
          this.blockMarker = -1; // invalidate
          this.leftRecord = null; // invalidate
        }
        return true;
      }
    }


    /**
     * Returns whether the right table can advance to the next record while maintaining equality.
     * Kind of like a peek() function
     **/
    private boolean advanceRightTable() {
      /* DONE */
      Record oldRecord = this.rightRecord;
      int oldEntyNum = this.rightEntryNum;
      this.rightRecord = this.getNextRightRecordInPage();

      if (this.rightRecord == null || this.mergeCompare() != 0) {
        this.rightRecord = oldRecord;
        this.rightEntryNum = oldEntyNum;
        return false;
      }
      this.rightRecord = oldRecord;
      this.rightEntryNum = oldEntyNum;
      return true;
    }


    private Record getNextRightRecordInPage() {
      byte b, mask;
      for (int i = this.rightEntryNum; i < this.rightHeader.length * 8; i++) {
        b = this.rightHeader[i/8];
        mask = (byte) (1 << (7 - (i % 8)));
        if ((b & mask) != (byte) 0) { // record here
          Schema schema;
          try {
            schema = SortMergeOperator.this.getTransaction().getSchema(this.rightTableName);
          } catch (DatabaseException e){
            System.out.println("Error attempting to get schema from right table in getNextRightRecordInPage");
            return null;
          }

          int entrySize = schema.getEntrySize();
          int headerSize;
          try {
            headerSize = SortMergeOperator.this.getHeaderSize(this.rightTableName);
          } catch (DatabaseException e) {
            System.out.println("Error attempting to get header size in getNextRightRecordInPage");
            return null;
          }

          int offset = headerSize + (entrySize * i);
          byte[] bytes = this.rightPage.readBytes(offset, entrySize);
          Record record = schema.decode(bytes);

          this.rightEntryNum++; // setup for next call
          return record;
        } else {
          this.rightEntryNum++;
        }
      }
      this.rightEntryNum = 0;
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

    public int mergeCompare() {
      return this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
              this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
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
