package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                           QueryOperator rightSource,
                           String leftColumnName,
                           String rightColumnName,
                           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private String[] leftPartitions;
    private String[] rightPartitions;
    /* Added */
    private HashMap<DataBox, List<Record>> hashTable;
    private int partitionsIndex;
    private Record nextRecord;
    private ListIterator<Record> hashListIterator;
    private List<Record> recordList;
    private Record rightRecord;

    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];
      rightPartitions = new String[numBuffers - 1];
      String leftTableName;
      String rightTableName;
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
        leftPartitions[i] = leftTableName;
        rightPartitions[i] = rightTableName;
      }
      /* DONE */

      // partition phase
      while (this.leftIterator.hasNext()) {
        Record record = this.leftIterator.next();
        DataBox leftJoinValue = record.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
        int index = leftJoinValue.hashCode() % (numBuffers - 1);
        addRecord(leftPartitions[index], record.getValues());
      }

      while (this.rightIterator.hasNext()) {
        Record record = this.rightIterator.next();
        DataBox rightJoinValue = record.getValues().get(GraceHashOperator.this.getRightColumnIndex());
        int index = rightJoinValue.hashCode() % (numBuffers - 1);
        addRecord(rightPartitions[index], record.getValues());
      }

      this.hashTable = null;
      this.partitionsIndex = 0;
      this.nextRecord = null;
      this.hashListIterator = null;
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
        /* handle hash table reset */
        if (this.hashTable == null) {
          // handle end of right table partition
          if (this.partitionsIndex >= this.rightPartitions.length) {
            return false;
          }

          this.hashTable = new HashMap<DataBox, List<Record>>();
          String leftTableName = this.leftPartitions[this.partitionsIndex];
          try {
            this.leftIterator = GraceHashOperator.this.getTableIterator(leftTableName);
          } catch (DatabaseException e) {
            System.out.println("Error instantiating iterator for " + leftTableName);
            return false;
          }

          String rightTableName = this.rightPartitions[this.partitionsIndex];
          try {
            this.rightIterator = GraceHashOperator.this.getTableIterator(rightTableName);
          } catch (DatabaseException e) {
            System.out.println("Error instantiating iterator for " + rightTableName);
            return false;
          }

          /* populate hash table from new left table partition */
          Record record;
          while (this.leftIterator.hasNext()) {
            record = this.leftIterator.next();
            DataBox databox = record.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
            List<Record> recordList = hashTable.get(databox);
            if (recordList == null) {
              recordList = new ArrayList<Record>();
              recordList.add(record);
              hashTable.put(databox, recordList);
            } else {
              recordList.add(record);
            }
          }

          this.partitionsIndex++;
          this.recordList = null;
        }

        /* handle end of a linked list */
        if (this.recordList == null) {
          if (!this.rightIterator.hasNext()) {
            this.hashTable = null;
            continue;
          }
          this.rightRecord = this.rightIterator.next();
          DataBox databox = this.rightRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
          this.recordList = this.hashTable.get(databox);
          if (this.recordList == null) {
            continue;
          } else {
            this.hashListIterator = this.recordList.listIterator();
          }
        }

        /* probe phase */
        if (!this.hashListIterator.hasNext()) {
          this.recordList = null;
          continue;
        }

        Record leftRecord = this.hashListIterator.next();
        List<DataBox> leftValues = new ArrayList<DataBox>(leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
        leftValues.addAll(rightValues);
        this.nextRecord = new Record(leftValues);
        return true;
      }
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
