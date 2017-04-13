package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Arrays;
import java.nio.ByteBuffer;

/**
 * A B+ tree leaf node entry.
 *
 * Properties:
 * `key`: the search key
 * `rid`: RecordID of a record containing the search key
 */
public class LeafEntry extends BEntry {
    private RecordID rid;


    public LeafEntry(DataBox key, RecordID rid) {
        super(key);
        this.rid = rid;
    }

    public LeafEntry(DataBox keySchema, byte[] buff) {
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
        byte[] rBytes = Arrays.copyOfRange(buff, keySchema.getSize(), keySchema.getSize() + RecordID.getSize());
        this.rid = new RecordID(rBytes);
    }

    @Override
    public RecordID getRecordID() {
        return rid;
    }

    @Override
    public byte[] toBytes() {
        byte[] keyBytes = getKey().getBytes();
        byte[] ridBytes = this.rid.getBytes();
        return ByteBuffer.allocate(keyBytes.length + ridBytes.length).put(keyBytes).put(ridBytes).array();
    }
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LeafEntry)) {
            return false;
        }

        LeafEntry otherLE = (LeafEntry) other;
        return otherLE.getKey().equals(this.getKey()) && otherLE.rid == this.rid;
    }

    @Override
    public String toString() {
        return this.getKey() + ", <" + rid + ">";
    }

    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new BPlusTreeException("Object does not match");
        }

        LeafEntry other = (LeafEntry) obj;

        if (!other.getKey().type().equals(this.getKey().type())) {
            throw new BPlusTreeException("DataBoxs in LeafEntry compareTo do not match");
        }

        if (other.getKey().getSize() != this.getKey().getSize()) {
            throw new BPlusTreeException("DataBoxs in LeafEntry compareTo have differing sizes");
        }
        int keyCompVal = this.getKey().compareTo(other.getKey());

        if (keyCompVal == 0) {
            return this.getRecordID().compareTo(other.getRecordID());
        }
        return keyCompVal;
    }
}
