package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.index.BPlusTree.NodeData;

import java.util.*;

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
        int keyPage = getFirstChild();  // Default keyPage
        List<BEntry> entries = getAllValidEntries();
        for (BEntry ent : entries) {
            if (key.compareTo(ent.getKey()) < 0) {
                break;
            }
            keyPage = ent.getPageNum();
        }
        return keyPage;
    }

    /**
     * Helper: overloaded function to handle tracking index
     * @param key the given key
     * @param data the NodeData object with index to update
     * @return page number of the child of this InnerNode whose subtree
     * contains the given key
     */
    public int findChildFromKey(DataBox key, NodeData data) {
        data.setIndex(-1);
        int keyPage = getFirstChild();  // Default keyPage
        List<BEntry> entries = getAllValidEntries();
        for (BEntry ent : entries) {
            if (key.compareTo(ent.getKey()) <= 0) {
                break;
            }
            keyPage = ent.getPageNum();
            data.incrementIndex();
        }
        return keyPage;
    }

    /**
     * Inserts a LeafEntry into the corresponding LeafNode in this subtree.
     *
     * @param ent the LeafEntry to be inserted
     * @return the InnerEntry to be pushed/copied up to this InnerNode's parent
     * as a result of this InnerNode being split, null otherwise
     */
    public InnerEntry insertBEntry(LeafEntry ent) {
        // Implement me!
        DataBox key = ent.getKey();
        int childPID = this.findChildFromKey(key);
        BPlusNode child = BPlusNode.getBPlusNode(this.getTree(), childPID);
//        System.out.println("Child is leaf: " + child.isLeaf());
        InnerEntry pushupEntry = child.insertBEntry(ent);

        if (pushupEntry != null) { // need to insert innerNode into self
            if (this.hasSpace()) {
                List<BEntry> newEntries = this.insertOrdered(pushupEntry);
                this.overwriteBNodeEntries(newEntries);
                return null;
            } else { // need to split, create new entry pointing to self, and return it
                return this.splitNode(pushupEntry);
            }
        }
        return null;
    }

    /**
     * Helper function to insert into list array in sorted order
     * @param entry the entry to be inserted
     */
    public List<BEntry> insertOrdered(BEntry entry) {
        List<BEntry> leafEntries = this.getAllValidEntries();
        int i = 0;

        for (BEntry leafEntry : leafEntries) {
            if (entry.compareTo(leafEntry) == -1) { // entry less than leafEntry
                leafEntries.add(i, entry);
                break;
            }
            i++;
        }

        if (i == leafEntries.size()) { // insert into the end
            leafEntries.add(entry);
        }

        return leafEntries;
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
        List<BEntry> nodeEntries = this.insertOrdered(newEntry);
        int len = nodeEntries.size();

        // handle left node
        List<BEntry> leftEntries = nodeEntries.subList(0, len / 2);
        this.overwriteBNodeEntries(leftEntries);

        // handle right node
        List<BEntry> rightEntries = nodeEntries.subList((len + 1)/ 2, len);
        InnerNode rightNode = new InnerNode(this.getTree());
        rightNode.setFirstChild(nodeEntries.get(len / 2).getPageNum());
        rightNode.overwriteBNodeEntries(rightEntries);

        // increment node count, point center entry to right node
//        this.getTree().incrementNumNodes();
        return new InnerEntry(nodeEntries.get(len / 2).key, rightNode.getPageNum());
    }
}
