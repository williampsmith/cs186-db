package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    /* TODO: Implement me! */
    return -1;
  }


  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
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
    private int pageInBlock;
    private int numPagesInBlock;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator)BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (BNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator)BNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      this.leftIterator = BNLJOperator.this.getPageIterator(this.leftTableName);
      this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
      this.nextRecord = null;
      this.leftEntryNum = 0;
      this.rightEntryNum = 0;
      this.block = new Page[numBuffers - 2];
      this.pageInBlock = 0;
      this.numPagesInBlock = 0;
      if (this.leftIterator.hasNext()) {
        assert(this.leftIterator.next().getPageNum() == 0);
        for (int i = 0; i < numBuffers - 2; i++) {
          if (this.leftIterator.hasNext()) {
            this.block[i] = this.leftIterator.next();
            this.numPagesInBlock++;
          }
        }
        if (this.block[this.pageInBlock] != null) {
          this.leftPage = this.block[this.pageInBlock];
          this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
          this.leftRecord = getNextLeftRecordInBlock();
        }
      }
      if (this.rightIterator.hasNext()) {
        assert(this.rightIterator.next().getPageNum() == 0);
        if (this.rightIterator.hasNext()) {
          this.rightPage = this.rightIterator.next();
          this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
          this.rightRecord = getNextRightRecordInPage();
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
        if (this.rightRecord == null) {
          this.leftRecord = getNextLeftRecordInBlock();
          if (this.leftRecord == null) {
            if (this.rightIterator.hasNext()) {
              this.rightPage = this.rightIterator.next();
              this.rightEntryNum = 0;
              this.leftEntryNum = 0;
              this.pageInBlock = 0;
              this.leftPage = this.block[this.pageInBlock];
              try {
                this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
                this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
              } catch (DatabaseException d) {
                return false;
              }

              this.leftRecord = getNextLeftRecordInBlock();
              this.rightRecord = getNextRightRecordInPage();
            } else {
              if (this.leftIterator.hasNext()) {
                this.pageInBlock = 0;
                this.numPagesInBlock = 0;
                for (int i = 0; i < numBuffers - 2; i++) {
                  if (this.leftIterator.hasNext()) {
                    this.block[i] = this.leftIterator.next();
                    this.numPagesInBlock++;
                  }
                }
                try {
                  this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
                  this.rightIterator.next();
                  this.rightPage = this.rightIterator.next();
                  this.leftPage = this.block[this.pageInBlock];
                  this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
                  this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
                } catch (DatabaseException d) {
                  return false;
                }
                this.leftEntryNum = 0;
                this.rightEntryNum = 0;
                this.leftRecord = getNextLeftRecordInBlock();
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
        DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
        DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
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

    private Record getNextLeftRecordInBlock() {
      try {
        while (this.pageInBlock < this.numPagesInBlock) {
          while (this.leftEntryNum < BNLJOperator.this.getNumEntriesPerPage(this.leftTableName)) {
            byte b = leftHeader[this.leftEntryNum / 8];
            int bitOffset = 7 - (this.leftEntryNum % 8);
            byte mask = (byte) (1 << bitOffset);
            byte value = (byte) (b & mask);
            if (value != 0) {
              int entrySize = BNLJOperator.this.getEntrySize(this.leftTableName);
              int offset = BNLJOperator.this.getHeaderSize(this.leftTableName) + (entrySize * this.leftEntryNum);
              byte[] bytes = this.leftPage.readBytes(offset, entrySize);
              Record toRtn = BNLJOperator.this.getLeftSource().getOutputSchema().decode(bytes);
              this.leftEntryNum++;
              return toRtn;
            }
            this.leftEntryNum++;
          }
          this.pageInBlock++;
          this.leftEntryNum = 0;
          if (this.pageInBlock < this.numPagesInBlock) {
            this.leftPage = this.block[this.pageInBlock];
            this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
          }
        }
      } catch (DatabaseException d)  {
        return null;
      }
      return null;
    }

    private Record getNextRightRecordInPage() {
      try {
        while (this.rightEntryNum < BNLJOperator.this.getNumEntriesPerPage(this.rightTableName)) {
          byte b = rightHeader[this.rightEntryNum / 8];
          int bitOffset = 7 - (this.rightEntryNum % 8);
          byte mask = (byte) (1 << bitOffset);
          byte value = (byte) (b & mask);
          if (value != 0) {
            int entrySize = BNLJOperator.this.getEntrySize(this.rightTableName);
            int offset = BNLJOperator.this.getHeaderSize(this.rightTableName) + (entrySize * rightEntryNum);
            byte[] bytes = this.rightPage.readBytes(offset, entrySize);
            Record toRtn = BNLJOperator.this.getRightSource().getOutputSchema().decode(bytes);
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