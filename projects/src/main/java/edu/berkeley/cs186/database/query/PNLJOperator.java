package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    /* TODO: Implement me! */
    return -1;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator implements Iterator<Record> {
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

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
        PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (PNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
        PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      this.leftIterator = PNLJOperator.this.getPageIterator(this.leftTableName);
      this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
      this.nextRecord = null;
      this.leftEntryNum = 0;
      this.rightEntryNum = 0;
      if (this.leftIterator.hasNext()) {
        assert (this.leftIterator.next().getPageNum() == 0);
        if (this.leftIterator.hasNext()) {
          this.leftPage = this.leftIterator.next();
          this.leftHeader = PNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
          this.leftRecord = getNextLeftRecordInPage();
        }
      }
      if (this.rightIterator.hasNext()) {
        assert(this.rightIterator.next().getPageNum() == 0);
        if (this.rightIterator.hasNext()) {
          this.rightPage = this.rightIterator.next();
          this.rightHeader = PNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
          this.rightRecord = getNextRightRecordInPage();
        }
      }
    }

    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }
      if (this.leftRecord == null || this.leftPage == null || this.rightPage == null) {
        return false;
      }
      while (true) {
        if (this.rightRecord == null) {
          this.leftRecord = getNextLeftRecordInPage();
          if (this.leftRecord == null) {
            if (this.rightIterator.hasNext()) {
              this.rightPage = this.rightIterator.next();
              this.rightEntryNum = 0;
              this.leftEntryNum = 0;
              try {
                this.rightHeader = PNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
              } catch (DatabaseException d) {
                return false;
              }
              this.leftRecord = getNextLeftRecordInPage();
              this.rightRecord = getNextRightRecordInPage();
            } else {
              if (this.leftIterator.hasNext()) {
                this.leftPage = this.leftIterator.next();
                try {
                  this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
                  this.rightIterator.next();
                  this.rightPage = this.rightIterator.next();
                  this.leftHeader = PNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
                  this.rightHeader = PNLJOperator.this.getPageHeader(this.leftTableName, this.rightPage);
                } catch (DatabaseException d) {
                  return false;
                }
                this.leftEntryNum = 0;
                this.rightEntryNum = 0;
                this.leftRecord = getNextLeftRecordInPage();
                this.rightRecord = getNextRightRecordInPage();
              } else {
                return false;
              }
            }
          } else {
            this.rightEntryNum = 0;
            this.rightRecord = getNextRightRecordInPage();
          }
        }
        DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
        DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
        if (leftJoinValue.equals(rightJoinValue)) {
          List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
          List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
          leftValues.addAll(rightValues);
          this.nextRecord = new Record(leftValues);
          this.rightRecord = getNextRightRecordInPage();
          return true;
        }
        this.rightRecord = getNextRightRecordInPage();
      }
    }

    private Record getNextLeftRecordInPage() {
      try {
        while (this.leftEntryNum < PNLJOperator.this.getNumEntriesPerPage(this.leftTableName)) {
          byte b = leftHeader[this.leftEntryNum / 8];
          int bitOffset = 7 - (this.leftEntryNum % 8);
          byte mask = (byte) (1 << bitOffset);
          byte value = (byte) (b & mask);
          if (value != 0) {
            int entrySize = PNLJOperator.this.getEntrySize(this.leftTableName);
            int offset = PNLJOperator.this.getHeaderSize(this.leftTableName) + (entrySize * this.leftEntryNum);
            byte[] bytes = this.leftPage.readBytes(offset, entrySize);
            Record toRtn = PNLJOperator.this.getLeftSource().getOutputSchema().decode(bytes);
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
        while (this.rightEntryNum < PNLJOperator.this.getNumEntriesPerPage(this.rightTableName)) {
          byte b = rightHeader[this.rightEntryNum / 8];
          int bitOffset = 7 - (this.rightEntryNum % 8);
          byte mask = (byte) (1 << bitOffset);
          byte value = (byte) (b & mask);
          if (value != 0) {
            int entrySize = PNLJOperator.this.getEntrySize(this.rightTableName);
            int offset = PNLJOperator.this.getHeaderSize(this.rightTableName) + (entrySize * rightEntryNum);
            byte[] bytes = this.rightPage.readBytes(offset, entrySize);
            Record toRtn = PNLJOperator.this.getRightSource().getOutputSchema().decode(bytes);
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
  }
}