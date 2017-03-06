package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * A leaf node of a B+ tree. A LeafNode header contains an `isLeaf` flag set
 * to 1. A LeafNode contains LeafEntries.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class LeafNode extends BPlusNode {

    public static int headerSize = 1;       // isLeaf

    public LeafNode(BPlusTree tree) {
        super(tree, true);
        tree.incrementNumNodes();
        getPage().writeByte(0, (byte) 1);   // isLeaf = 1
    }

    public LeafNode(BPlusTree tree, int pageNum) {
        super(tree, pageNum, true);
        if (getPage().readByte(0) != (byte) 1) {
            throw new BPlusTreeException("Page is not Leaf Node!");
        }
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    /**
     * Inserts a LeafEntry into this LeafNode.
     *
     * @param ent the LeafEntry to be inserted
     * @return the InnerEntry to be pushed/copied up to this LeafNode's parent
     * as a result of this LeafNode being split, null otherwise
     */
    @Override
    public InnerEntry insertBEntry(LeafEntry ent) {
        if (this.hasSpace()) {
            List<BEntry> newEntries = this.insertOrdered(ent);
            this.overwriteBNodeEntries(newEntries);
            return null;
        } else { // need to split, create new entry pointing to self, and return it
            return this.splitNode(ent);
        }
    }

    /**
     * Helper function to insert into node in sorted order
     * @param entry the entry to be inserted
     */
    public List<BEntry> insertOrdered(BEntry entry) {
        List<BEntry> nodeEntries = this.getAllValidEntries();
        int i = 0;

        for (BEntry nodeEntry : nodeEntries) {
            if (entry.compareTo(nodeEntry) == -1) { // entry less than nodeEntry
                nodeEntries.add(i, entry);
                break;
            }
            i++;
        }

        if (i == nodeEntries.size()) { // insert into the end
            nodeEntries.add(entry);
        }

        return nodeEntries;
    }

    /**
     * Splits this LeafNode and returns the resulting InnerEntry to be
     * pushed/copied up to this LeafNode's parent as a result of the split.
     * The left node should contain d entries and the right node should contain
     * d+1 entries.
     *
     * @param newEntry the BEntry that is being added to this LeafNode
     * @return the resulting InnerEntry to be pushed/copied up to this
     * LeafNode's parent as a result of this LeafNode being split
     */
    @Override
    public InnerEntry splitNode(BEntry newEntry) {
        // Implement me!
        List<BEntry> nodeEntries = this.insertOrdered(newEntry); // insert into list
        int len = nodeEntries.size();

        // handle left node
        List<BEntry> leftEntries = nodeEntries.subList(0, len / 2);
        this.overwriteBNodeEntries(leftEntries);

        // handle right node
        List<BEntry> rightEntries = nodeEntries.subList((len / 2), len);
        LeafNode rightNode = new LeafNode(this.getTree());
        rightNode.overwriteBNodeEntries(rightEntries);

        // increment node count, point center entry to right node
//        this.getTree().incrementNumNodes();
        return new InnerEntry(nodeEntries.get(len / 2).key, rightNode.getPageNum());
    }

    public InnerEntry splitNodeCheck(BEntry newEntry) {
        // DONE
        int totalNumEntries = this.numEntries + 1;
        int d = totalNumEntries / 2;
        List<BEntry> allEntriesSorted = this.insertOrdered(newEntry);
        List<BEntry> leftEntries = new ArrayList<BEntry>();
        List<BEntry> rightEntries = new ArrayList<BEntry>();

        for (int i = 0; i < d; i++) {
            leftEntries.add(allEntriesSorted.get(i));
        }
        for (int i = d; i < totalNumEntries; i++) {
            rightEntries.add(allEntriesSorted.get(i));
        }

        this.overwriteBNodeEntries(leftEntries);
        LeafNode newRightLeafNode = new LeafNode(this.getTree());
        newRightLeafNode.overwriteBNodeEntries(rightEntries);

        return new InnerEntry(rightEntries.get(0).getKey(), newRightLeafNode.getPageNum());
    }


    /**
     * Creates an iterator of RecordIDs for all entries in this node.
     *
     * @return an iterator of RecordIDs
     */
    public Iterator<RecordID> scan() {
        List<BEntry> validEntries = getAllValidEntries();
        List<RecordID> rids = new ArrayList<RecordID>();

        for (BEntry le : validEntries) {
            rids.add(le.getRecordID());
        }

        return rids.iterator();
    }

    /**
     * Creates an iterator of RecordIDs whose keys are greater than or equal to
     * the given start value key.
     *
     * @param startValue the start value key
     * @return an iterator of RecordIDs
     */
    public Iterator<RecordID> scanFrom(DataBox startValue) {
        List<BEntry> validEntries = getAllValidEntries();
        List<RecordID> rids = new ArrayList<RecordID>();

        for (BEntry le : validEntries) {
            if (startValue.compareTo(le.getKey()) < 1) {
                rids.add(le.getRecordID());
            }
        }
        return rids.iterator();
    }

    /**
     * Creates an iterator of RecordIDs that correspond to the given key in the
     * current leafNode Page.
     *
     * @param key the search key
     * @return an iterator of RecordIDs
     */
    public Iterator<RecordID> scanForKey(DataBox key) {
        List<BEntry> validEntries = getAllValidEntries();
        List<RecordID> rids = new ArrayList<RecordID>();

        for (BEntry le : validEntries) {
            if (key.compareTo(le.getKey()) == 0) {
                rids.add(le.getRecordID());
            }
        }
        return rids.iterator();
    }

    public boolean containsKey(DataBox key) {
        List<BEntry> validEntries = getAllValidEntries();

        for (BEntry le : validEntries) {
            if (key.compareTo(le.getKey()) == 0) {
                return true;
            }
        }
        return false;

    }
}
