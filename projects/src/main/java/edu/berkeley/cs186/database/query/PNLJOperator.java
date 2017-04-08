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
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator implements Iterator<Record> {
    /* Suggested Fields */
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
      /* Suggested Starter Code: get table names. */
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

      /* DONE */
      this.leftIterator = PNLJOperator.this.getPageIterator(this.leftTableName);
      this.leftIterator.next(); // discard header page, which must be present
      this.leftPage = null;
      this.leftHeader = null;
      this.leftEntryNum = 0;
      this.leftRecord = null;

      this.rightIterator = null;
      this.rightPage = null;
      this.rightHeader = null;
      this.rightEntryNum = 0;
      this.rightRecord = null;

      this.nextRecord = null;
    }

    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }

      while (true) {
        /* handle left page reset */
        if (this.leftPage == null) { // must get a new page. Find first nonempty
          if (!this.leftIterator.hasNext()) { // no more left pages
            return false;
          }

          this.leftPage = this.leftIterator.next();
          this.leftEntryNum = 0;
          try {
            this.leftHeader = PNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
          } catch (DatabaseException e) {
            System.out.println("Could not retrieve page header for left page " + this.leftPage);
            continue;
          }

          // setup right side to the beginning of inner loop since we are now on a new outer loop page
          try {
            this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
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
          if (!this.rightIterator.hasNext()) { // no more right pages, need to go to next left page
            this.leftPage = null;
            continue;
          }

          this.rightPage = this.rightIterator.next();
          this.rightEntryNum = 0;
          try {
            this.rightHeader = PNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
          } catch (DatabaseException e) {
            System.out.println("Could not retrieve page header for right page " + this.rightPage);
            continue;
          }

          this.leftRecord = null;
          this.rightRecord = null;
        }

        /* handle left record reset */
        if (this.leftRecord == null) {
          this.leftRecord = this.getNextLeftRecordInPage();
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
        DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
        DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
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


    /* Returns null if there are no more records on the page starting at leftEntryNum */
    private Record getNextLeftRecordInPage() {
      byte b, mask;
      for (int i = this.leftEntryNum; i < this.leftHeader.length * 8; i++) {
        b = this.leftHeader[i/8];
        mask = (byte) (1 << (7 - (i % 8)));
        if ((b & mask) != (byte) 0) { // record here
          Schema schema;
          try {
            schema = PNLJOperator.this.getTransaction().getSchema(this.leftTableName);
          } catch (DatabaseException e){
            System.out.println("Error attempting to get schema from left table in getNextLeftRecordInPage");
            return null;
          }

          int entrySize = schema.getEntrySize();
          int headerSize;
          try {
            headerSize = PNLJOperator.this.getHeaderSize(this.leftTableName);
          } catch (DatabaseException e) {
            System.out.println("Error attempting to get header size in getNextLeftRecordInPage");
            return null;
          }

          int offset = headerSize + (entrySize * i);
          byte[] bytes = this.leftPage.readBytes(offset, entrySize);
          Record record = schema.decode(bytes);

          this.leftEntryNum++; // setup for next call
          return record;
        } else {
          this.leftEntryNum++;
        }
      }
      this.leftEntryNum = 0;
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
            schema = PNLJOperator.this.getTransaction().getSchema(this.rightTableName);
          } catch (DatabaseException e){
            System.out.println("Error attempting to get schema from right table in getNextRightRecordInPage");
            return null;
          }

          int entrySize = schema.getEntrySize();
          int headerSize;
          try {
            headerSize = PNLJOperator.this.getHeaderSize(this.rightTableName);
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
