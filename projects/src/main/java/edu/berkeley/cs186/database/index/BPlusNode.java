package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * A B+ tree node. A node is represented as a page with a page header, entry
 * bitmap, and entries. The type of page header and entry are determined by the
 * subclasses InnerNode and LeafNode.
 *
 * Properties:
 * keySchema: DataBox for this index's search key
 * entrySize: physical size (in bytes) of the page entry of this node
 * numEntries: number of entries this node can hold
 * bitMapSize: physical size (in bytes) of a page header entry bitmap
 * headerSize: physical size (in bytes) of the rest of the page header
 * tree`: BPlusTree containing this node
 * pageNum`: page number corresponding to this node
 */
public abstract class BPlusNode {
    private DataBox keySchema;

    private int entrySize;
    protected int numEntries;
    private int bitMapSize;
    private int headerSize;
    private BPlusTree tree;
    private int pageNum;

    /**
     * Abstract constructor for BPlusNode for existing nodes.
     *
     * @param tree the BPlusTree this Node belongs to
     * @param pageNum the pageNum select to open this node on
     * @param isLeaf is this node a leaf
     */

    public BPlusNode(BPlusTree tree, int pageNum, boolean isLeaf) {
        this.keySchema = tree.keySchema;
        this.tree = tree;
        this.pageNum = pageNum;
        if (isLeaf) {
            this.headerSize = LeafNode.headerSize;
            this.entrySize = keySchema.getSize() + RecordID.getSize();
        } else {
            this.headerSize = InnerNode.headerSize;
            this.entrySize = keySchema.getSize() + 4;
        }

        this.bitMapSize = (8 * (Page.pageSize - 5) / (1 + 8 * this.entrySize)) / 8;
        this.numEntries = bitMapSize * 8;
    }

    /**
     * Abstract constructor for BPlusNode for new nodes.
     * Auto-allocates a Page for this node.
     *
     * @param tree the BPlusTree this Node belongs to
     * @param isLeaf is this node a leaf
     */
    public BPlusNode(BPlusTree tree, boolean isLeaf) {
        this(tree, tree.allocator.allocPage(), isLeaf);
    }

    /**
     * Return an existing Node from a BPlusTree and pageNum.
     *
     * @param tree the BPlusTree this Node belongs to
     * @param pageNum the page number on select this node exists on
     * @return BPlusNode object that exists on this Page
     */
    public static BPlusNode getBPlusNode(BPlusTree tree, int pageNum) {
        if (tree.allocator.fetchPage(pageNum).readByte(0) == (byte) 0) {
            return new InnerNode(tree, pageNum);
        }
        return new LeafNode(tree, pageNum);
    }

    /**
     * Fetch Page this BPlusNode exists on.
     *
     * @return the Page that this BPlusNode exists on
     */
    public Page getPage() {
        return tree.allocator.fetchPage(this.pageNum);
    }

    public int getPageNum() {
        return pageNum;
    }

    public boolean hasSpace() {
        return findFreeEntry() > -1;
    }

    /**
     * Retrieve the BPlusTree that this BPlusNode belongs to.
     *
     * @return the BPlusTree that owns this node
     */
    public BPlusTree getTree() {
        return tree;
    }

    /**
     * Split BPlusNode driven by new entries.
     */
    public InnerEntry splitNode(BEntry newEntry) {
        throw new BPlusTreeException("Not Implemented");
    }

    public boolean isLeaf() {
        throw new BPlusTreeException("Not Implemented");
    }

    private byte[] getBitMap() {
        return getPage().readBytes(headerSize, bitMapSize);
    }

    private void setBitMap(byte[] bitMap) {
        getPage().writeBytes(headerSize, bitMapSize, bitMap);
    }

    /**
     * @param entryNum position of bit on bitmap
     * @return starting byte of entry indicated by entryNum
     */

    private int getOffset(int entryNum) {
        return entryNum*entrySize + this.headerSize + this.bitMapSize;
    }

    /**
     * Write a BEentry into the entryNum specified.
     *
     * @param entryNum the entry number to fill
     * @param ent the BEntry to write
     */
    private void writeEntry(int entryNum, BEntry ent) {
        int byteOffset = entryNum/8;
        int bitOffset = 7 - (entryNum % 8);
        byte mask = (byte) (1 << bitOffset);

        byte[] bitMap = getBitMap();
        bitMap[byteOffset] = (byte) (bitMap[byteOffset] | mask);
        setBitMap(bitMap);
        int entryOffset = getOffset(entryNum);
        getPage().writeBytes(entryOffset, entrySize, ent.toBytes());
    }

    /**
     * Read a BEntry from the entryNum specified.
     *
     * @param entryNum the entry number to read from
     * @return the BEntry corresponding to the entryNum
     */
    private BEntry readEntry(int entryNum) {
        if (isLeaf()) {
            return new LeafEntry(this.keySchema, getPage().readBytes(getOffset(entryNum), entrySize));
        } else {
            return new InnerEntry(this.keySchema, getPage().readBytes(getOffset(entryNum), entrySize));
        }
    }

    /**
     * Return the entry number of the first free entry of this node.
     *
     * @return the first free entry number, otherwise -1 if none exists
     */
    private int findFreeEntry() {
        byte[] bitMap = this.getBitMap();

        for (int i = 0; i < this.numEntries; i++) {
            int byteOffset = i/8;
            int bitOffset = 7 - (i % 8);
            byte mask = (byte) (1 << bitOffset);
            byte value = (byte) (bitMap[byteOffset] & mask);
            if (value == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Return a list of valid, existing entries of this node.
     *
     * @return a list of entries that have the valid bit set
     */
    protected List<BEntry> getAllValidEntries() {
        byte[] bitMap = this.getBitMap();
        List<BEntry> entries = new ArrayList<BEntry>();
        for (int i = 0; i < this.numEntries; i++) {
            int byteOffset = i/8;
            int bitOffset = 7 - (i % 8);
            byte mask = (byte) (1 << bitOffset);

            byte value = (byte) (bitMap[byteOffset] & mask);

            if (value != 0) {
                entries.add(readEntry(i));
            }
        }
        return entries;
    }

    /**
     * Clear all the entries of this node, and write all the given entries into
     * the node, starting from the first entry number.
     *
     * @param entries the list of entries to write
     */
    protected void overwriteBNodeEntries(List<BEntry> entries) {
        byte[] zeros = new byte[bitMapSize];
        setBitMap(zeros);
        if (entries.size() > numEntries) {
            throw new BPlusTreeException("too many BEntry given to fit on page");
        }

        for (int i = 0; i < entries.size(); i++) {
            writeEntry(i, entries.get(i));
        }
    }

    /**
     * Insert an entry into this node.
     *
     * @param ent the entry to insert
     */
    public InnerEntry insertBEntry(LeafEntry ent) {
        throw new BPlusTreeException("Not Implemented");
    }
}
