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
        if (hasSpace()) {
            List<BEntry> validEntries = getAllValidEntries();
            validEntries.add(ent);
            Collections.sort(validEntries);
            overwriteBNodeEntries(validEntries);
            return null;
        } else {
            return splitNode(ent);
        }
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
        List<BEntry> validEntries = getAllValidEntries();
        validEntries.add(newEntry);
        Collections.sort(validEntries);

        List<BEntry> leftNodeEntries = validEntries.subList(0, validEntries.size()/2);
        BEntry middleEntry = validEntries.get(validEntries.size()/2);
        List<BEntry> rightNodeEntries = validEntries.subList(validEntries.size()/2, validEntries.size());

        overwriteBNodeEntries(leftNodeEntries);

        LeafNode rightNode = new LeafNode(getTree());
        rightNode.overwriteBNodeEntries(rightNodeEntries);

        InnerEntry newMiddleEntry = new InnerEntry(middleEntry.getKey(), rightNode.getPageNum());

        return newMiddleEntry;
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
