package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Arrays;
import java.nio.ByteBuffer;

public abstract class BEntry implements Comparable {
    protected DataBox key;

    public BEntry() {
    }


    public BEntry(DataBox keySchema) {
        key = keySchema;
    }

    public BEntry(DataBox keySchema, byte[] buff) {
        throw new BPlusTreeException("Not Implemented");
    }


    public DataBox getKey() {
        return key;
    }

    public int getPageNum() {
        throw new BPlusTreeException("Not Implemented");
    }

    public RecordID getRecordID() {
        throw new BPlusTreeException("Not Implemented");
    }


    public byte[] toBytes() {
        throw new BPlusTreeException("Not Implemented");
    }

    @Override
    public boolean equals(Object other) {
        throw new BPlusTreeException("Not Implemented");
    }

    @Override
    public String toString() {
        throw new BPlusTreeException("Not Implemented");
    }

    public int compareTo(Object obj) {
        throw new BPlusTreeException("Not Implemented");
    }
}
