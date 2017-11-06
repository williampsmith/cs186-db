package edu.berkeley.cs186.database.index;

import java.lang.RuntimeException;

public class BPlusTreeException extends RuntimeException {
    private String message;

    public BPlusTreeException(String message) {
        this.message = message;
    }

    public BPlusTreeException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
