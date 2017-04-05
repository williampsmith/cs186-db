package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.List;

/**
 * An inner node of a B+ tree. An InnerNode header contains an `isLeaf` flag
 * set to 0 and the page number of the first child node (or -1 if no child
 * exists). An InnerNode contains InnerEntries.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {
    public static int headerSize = 5;       // isLeaf + pageNum of first child

    public InnerNode(BPlusTree tree) {
        super(tree, false);
        tree.incrementNumNodes();
        getPage().writeByte(0, (byte) 0);   // isLeaf = 0
        setFirstChild(-1);
    }

    public InnerNode(BPlusTree tree, int pageNum) {
        super(tree, pageNum, false);
        if (getPage().readByte(0) != (byte) 0) {
            throw new BPlusTreeException("Page is not Inner Node!");
        }
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    public int getFirstChild() {
        return getPage().readInt(1);
    }

    public void setFirstChild(int val) {
        getPage().writeInt(1, val);
    }

    /**
     * Finds the correct child of this InnerNode whose subtree contains the
     * given key.
     *
     * @param key the given key
     * @return page number of the child of this InnerNode whose subtree
     * contains the given key
     */
    public int findChildFromKey(DataBox key) {
        int childToTraverse = getFirstChild();
        List<BEntry> validEntries = getAllValidEntries();
        for (BEntry entry : validEntries) {
            if (key.compareTo(entry.getKey()) < 0) {
                break;
            } else {
                childToTraverse = entry.getPageNum();
            }
        }
        return childToTraverse;
    }

    /**
     * Inserts a LeafEntry into the corresponding LeafNode in this subtree.
     *
     * @param ent the LeafEntry to be inserted
     * @return the InnerEntry to be pushed/copied up to this InnerNode's parent
     * as a result of this InnerNode being split, null otherwise
     */
    public InnerEntry insertBEntry(LeafEntry ent) {
        int childPageNum = findChildFromKey(ent.getKey());
        BPlusNode childNode = getBPlusNode(getTree(), childPageNum);
        InnerEntry pushedEntry = childNode.insertBEntry(ent);

        if (pushedEntry != null) {
            if (hasSpace()) {
                List<BEntry> validEntries = getAllValidEntries();
                validEntries.add(pushedEntry);
                Collections.sort(validEntries);
                overwriteBNodeEntries(validEntries);
                return null;
            } else {
                return splitNode(pushedEntry);
            }
        } else {
            return null;
        }
    }

    /**
     * Splits this InnerNode and returns the resulting InnerEntry to be
     * pushed/copied up to this InnerNode's parent as a result of the split.
     * The left node should contain d entries and the right node should contain
     * d entries.
     *
     * @param newEntry the BEntry that is being added to this InnerNode
     * @return the resulting InnerEntry to be pushed/copied up to this
     * InnerNode's parent as a result of this InnerNode being split
     */
    @Override
    public InnerEntry splitNode(BEntry newEntry) {
        List<BEntry> validEntries = getAllValidEntries();
        validEntries.add(newEntry);
        Collections.sort(validEntries);

        List<BEntry> leftNodeEntries = validEntries.subList(0, validEntries.size()/2);
        BEntry middleEntry = validEntries.get(validEntries.size()/2);
        List<BEntry> rightNodeEntries = validEntries.subList(validEntries.size()/2 + 1, validEntries.size());

        overwriteBNodeEntries(leftNodeEntries);

        InnerNode rightNode = new InnerNode(getTree());
        rightNode.setFirstChild(middleEntry.getPageNum());
        rightNode.overwriteBNodeEntries(rightNodeEntries);

        InnerEntry newMiddleEntry = new InnerEntry(middleEntry.getKey(), rightNode.getPageNum());

        return newMiddleEntry;
    }
}
