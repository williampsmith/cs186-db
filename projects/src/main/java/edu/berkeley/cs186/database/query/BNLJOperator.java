package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

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
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
    /* DONE */
    private int blockSize;
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page rightPage;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int leftEntryNum;
    private int leftBlockPageNum;
    private int rightEntryNum;
    private Page[] block; // should be of size this.blockSize

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (BNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) BNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }

      /* DONE */
      this.blockSize = BNLJOperator.this.numBuffers - 2;
      this.leftIterator = BNLJOperator.this.getPageIterator(this.leftTableName);
      this.leftIterator.next(); // discard header page, which must be present
      this.block = null;
      this.leftHeader = null;
      this.leftEntryNum = 0;
      this.leftBlockPageNum = 0;
      this.leftRecord = null;

      this.rightIterator = null;
      this.rightPage = null;
      this.rightHeader = null;
      this.rightEntryNum = 0;
      this.rightRecord = null;

      this.nextRecord = null;
    }

    /**
     * Should be called at the beginning of repopulating a new block.
     * This allows us to use the existence of null in the array to know
     * how many blocks are present.
     */
    public void nullifyBlock() {
      for (int i = 0; i < this.blockSize; i++) {
        this.block[i] = null;
      }
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.blockSize <= 0) {
        return false;
      }

      if (this.nextRecord != null) {
        return true;
      }

      while (true) {
        /* handle left block reset */
        if (this.block == null) { // must get a new block of pages
          if (!this.leftIterator.hasNext()) { // no more left pages
            return false;
          }

          this.block = new Page[this.blockSize];
          this.nullifyBlock();
          for (int i = 0; i < this.blockSize; i++) {
            this.block[i] = this.leftIterator.next();
            if (!this.leftIterator.hasNext()) {
              break;
            }
          }

          this.leftEntryNum = 0;
          this.leftBlockPageNum = 0;

          try {
            this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.block[0]);
          } catch (DatabaseException e) {
            System.out.println("Could not retrieve page header for left page 1");
            continue;
          }

          // setup right side to the beginning of inner loop since we are now on a new outer loop page
          try {
            this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
          } catch (DatabaseException e) {
            System.out.println("Error instantiating right table page iterator.");
            return false;
          }

          this.rightIterator.next(); // discard header page, which must be present
          this.rightPage = null; // flag reset of right page
          this.leftRecord = null;
          this.rightRecord = null;
        }

        /* handle right page reset */
        if (this.rightPage == null) {
          if (!this.rightIterator.hasNext()) { // no more right pages, need to go to next left block
            this.block = null;
            continue;
          }

          this.rightPage = this.rightIterator.next();
          this.rightEntryNum = 0;
          try {
            this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
          } catch (DatabaseException e) {
            System.out.println("Could not retrieve page header for right page " + this.rightPage);
            continue;
          }

          this.leftRecord = null;
          this.rightRecord = null;
        }

        /* handle left record reset */
        if (this.leftRecord == null) {
          this.leftRecord = this.getNextLeftRecordInBlock();
          if (this.leftRecord == null) {
            this.rightPage = null; // invalidate to go to next right page
            this.rightRecord = null; // need to go to first record in next right page
            continue;
          }
        }

        /* handle right record reset */
        if (this.rightRecord == null) {
          this.rightRecord = this.getNextRightRecordInPage();
          if (this.rightRecord == null) {
            this.leftRecord = null; // invalidate to go to next left record
            continue;
          }
        }

        /* left and right records set. Check for match */
        DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
        DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
        if (leftJoinValue.equals(rightJoinValue)) {
          List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
          List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
          leftValues.addAll(rightValues);
          this.nextRecord = new Record(leftValues);
          this.rightRecord = null;
          return true;
        }
        this.rightRecord = null; // invalidate to go to next right record
      }
    }

    private Record getNextLeftRecordInBlock() {
      byte b, mask;
      // retrieve the current page
      for (int j = this.leftBlockPageNum; j < this.blockSize; j++) {
        Page page = this.block[j];
        if (page == null) {
          this.leftBlockPageNum = 0;
          this.leftEntryNum = 0;
          return null;
        }

        for (int i = this.leftEntryNum; i < this.leftHeader.length * 8; i++) {
          b = this.leftHeader[i/8];
          mask = (byte) (1 << (7 - (i % 8)));
          if ((b & mask) != (byte) 0) { // record here
            Schema schema;
            try {
              schema = BNLJOperator.this.getTransaction().getSchema(this.leftTableName);
            } catch (DatabaseException e) {
              System.out.println("Error attempting to get schema from right table in getNextRightRecordInPage");
              return null;
            }

            int entrySize = schema.getEntrySize();
            int headerSize;
            try {
              headerSize = BNLJOperator.this.getHeaderSize(this.leftTableName);
            } catch (DatabaseException e) {
              System.out.println("Error attempting to get header size in getNextRightRecordInPage");
              return null;
            }

            int offset = headerSize + (entrySize * i);
            byte[] bytes = page.readBytes(offset, entrySize);
            Record record = schema.decode(bytes);

            this.leftEntryNum++; // setup for next call
            return record;
          } else {
            this.leftEntryNum++;
          }
        }
        this.leftEntryNum = 0;
        this.leftBlockPageNum++;
      }
      this.leftEntryNum = 0;
      this.leftBlockPageNum = 0;
      return null;
    }

    private Record getNextRightRecordInPage() {
      byte b, mask;
      for (int i = this.rightEntryNum; i < this.rightHeader.length * 8; i++) {
        b = this.rightHeader[i/8];
        mask = (byte) (1 << (7 - (i % 8)));
        if ((b & mask) != (byte) 0) { // record here
          Schema schema;
          try {
            schema = BNLJOperator.this.getTransaction().getSchema(this.rightTableName);
          } catch (DatabaseException e) {
            System.out.println("Error attempting to get schema from right table in getNextRightRecordInPage");
            return null;
          }

          int entrySize = schema.getEntrySize();
          int headerSize;
          try {
            headerSize = BNLJOperator.this.getHeaderSize(this.rightTableName);
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
  }
}
