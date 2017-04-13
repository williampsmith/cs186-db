package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.*;

import java.util.Arrays;
import java.nio.ByteBuffer;

/**
 * A B+ tree inner node entry.
 *
 * Properties:
 * `key`: the key which to split on for searching
 * `pageNum`: the page number of a child node
 */
public class InnerEntry extends BEntry {
    private int pageNum;

    public InnerEntry(DataBox key, int pageNum) {
        super(key);
        this.pageNum = pageNum;
    }

    public InnerEntry(DataBox keySchema, byte[] buff) {
        byte[] keyBytes = Arrays.copyOfRange(buff, 0, keySchema.getSize());
        DataBox.Types type = keySchema.type();
        switch(type) {
            case INT:
                this.key = new IntDataBox(keyBytes);
                break;
            case STRING:
                this.key = new StringDataBox(keyBytes);
                break;
            case BOOL:
                this.key = new BoolDataBox(keyBytes);
                break;
            case FLOAT:
                this.key = new FloatDataBox(keyBytes);
                break;
        }
        byte[] pBytes = Arrays.copyOfRange(buff, keySchema.getSize(), keySchema.getSize() + 4);
        this.pageNum = ByteBuffer.wrap(pBytes).getInt();
    }

    @Override
    public int getPageNum() {
        return this.pageNum;
    }

    public byte[] toBytes() {
        byte[] keyBytes = this.key.getBytes();
        byte[] pageNumBytes = ByteBuffer.allocate(4).putInt(this.pageNum).array();
        return ByteBuffer.allocate(keyBytes.length + pageNumBytes.length).put(keyBytes).put(pageNumBytes).array();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof InnerEntry)) {
            return false;
        }

        InnerEntry otherLE = (InnerEntry) other;
        return otherLE.key.equals(this.key) && otherLE.pageNum == this.pageNum;
    }

    @Override
    public String toString() {
        return key + " <" + pageNum + ">";
    }

    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new BPlusTreeException("Object does not match");
        }

        InnerEntry other = (InnerEntry) obj;

        if (!other.getKey().type().equals(this.getKey().type())) {
            throw new BPlusTreeException("DataBoxs in InnerEntry compareTo do not match");
        }

        if (other.getKey().getSize() != this.getKey().getSize()) {
            throw new BPlusTreeException("DataBoxs in InnerEntry compareTo have differing sizes");
        }
        int keyCompVal = this.getKey().compareTo(other.getKey());

        if (keyCompVal == 0) {
            return Integer.compare(this.getPageNum(), other.getPageNum());
        }
        return keyCompVal;
    }
}
