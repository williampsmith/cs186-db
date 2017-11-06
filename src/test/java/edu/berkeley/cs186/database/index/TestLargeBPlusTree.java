package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.StudentTestP2;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runners.MethodSorters;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import static org.junit.Assert.*;

public class TestLargeBPlusTree {
    public static final String testFile = "BPlusTreeTest";
    private BPlusTree bp;
    public static final int intLeafPageSize = 400;
    public static final int intInnPageSize = 496;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    // 160 seconds max per method tested.
    public Timeout globalTimeout = Timeout.seconds(160);

    @Before
    public void beforeEach() throws Exception {
        tempFolder.newFile(testFile);
        String tempFolderPath = tempFolder.getRoot().getAbsolutePath();
        this.bp = new BPlusTree(new IntDataBox(), testFile, tempFolderPath);
    }

    @Test
    public void testBPlusTreeInsert() {
        /** Insert records to separate pages in increasing pageNum order. */
        for (int i = 0; i < 10; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i,0));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        int expectedPageNum = 0;
        while (rids.hasNext()) {
            assertEquals(expectedPageNum, rids.next().getPageNum());
            expectedPageNum++;
        }
        assertEquals(1, this.bp.getNumNodes());
    }
}