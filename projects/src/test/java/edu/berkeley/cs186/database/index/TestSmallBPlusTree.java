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

public class TestSmallBPlusTree {
    public static final String testFile = "BPlusTreeTest";
    private BPlusTree bp;
    public static final int intLeafPageSize = 4;
    public static final int intInnPageSize = 4;

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

    /**
     * Test sample, do not modify.
     */
    @Test
    @Category(StudentTestP2.class)
    public void testSample() {
        assertEquals(true, true); // Do not actually write a test like this!
    }

    @Test
    public void testBPlusTreeInsert() {
        /** Insert records to separate pages in increasing pageNum order. */
        for (int i = 0; i < intLeafPageSize/2; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i, i));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        int expectedPageAndEntryNum = 0;
        while (rids.hasNext()) {
            RecordID rid = rids.next();
            assertEquals(expectedPageAndEntryNum, rid.getPageNum());
            assertEquals(expectedPageAndEntryNum, rid.getEntryNumber());
            expectedPageAndEntryNum++;
        }
        assertEquals((intLeafPageSize)/2, expectedPageAndEntryNum);
    }

    @Test
    public void testBPlusTreeInsertIterateFrom() {
        /**
         * Scan records starting from the middle page after inserting records
         * to separate pages in decreasing pageNum order.
         */
        for (int i = (2*intLeafPageSize)-1; i >= 0; i--) {
            bp.insertKey(new IntDataBox(i), new RecordID(i, 0));
        }
        Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataBox(intLeafPageSize));
        int expectedPageNum = intLeafPageSize;
        while (rids.hasNext()) {
            RecordID rid = rids.next();
            assertEquals(expectedPageNum, rid.getPageNum());
            expectedPageNum++;
        }
        assertEquals(2*intLeafPageSize, expectedPageNum);
    }

    @Test
    public void testBPlusTreeInsertIterateFullLeafNode() {
        /** Insert enough records to fill a leaf. */
        int pageNum = 1;
        for (int i = 0; i < intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(pageNum, i));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        int expectedEntryNum = 0;
        while (rids.hasNext()) {
            RecordID rid = rids.next();
            assertEquals(expectedEntryNum, rid.getEntryNumber());
            assertEquals(pageNum, rid.getPageNum());
            expectedEntryNum++;
        }
        assertEquals(intLeafPageSize, expectedEntryNum);
    }

    @Test
    public void testBPlusTreeInsertIterateFullLeafSplit() {
        /** Split a full leaf by inserting a leaf's page size + 1 records. */
        int pageNum = 1;
        for (int i = 0; i < intLeafPageSize + 1; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(pageNum, i));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        int expectedEntryNum = 0;
        while (rids.hasNext()) {
            RecordID rid = rids.next();
            assertEquals(expectedEntryNum, rid.getEntryNumber());
            assertEquals(pageNum, rid.getPageNum());
            expectedEntryNum++;
        }
        assertEquals(intLeafPageSize + 1, expectedEntryNum);
    }

    @Test
    public void testBPlusTreeInsertAppendIterateMultipleFullLeafSplit() {
        /**
         * Split leaves three times by inserting enough records for four
         * leaves: three full and one with a record.
         */
        for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i,0));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        int expectedPageNum = 0;
        while (rids.hasNext()) {
            RecordID rid = rids.next();
            assertEquals(expectedPageNum, rid.getPageNum());
            expectedPageNum++;
        }
        assertEquals(3*intLeafPageSize + 1, expectedPageNum);
    }


    @Test
    public void testFullPage() {
        /**
         * Insert four leaves in a sweeping fashion: three full and one with a
         * record.
         */
        for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
            bp.insertKey(new IntDataBox(i % 3), new RecordID(i % 3, i));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        for (int i = 0; i < intLeafPageSize + 1; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
        }
    }

    @Test
    public void testBPlusTreeSweepInsertSortedScanMultipleFullLeafSplit() {
        /**
         * Insert 3 full leafs of records plus one additional record.
         * Inserts are done in a sweeping fashion:
         *   1st insert of value 0 on (page 0, entry 0)
         *   2nd insert of value 1 on (page 1, entry 1)
         *   3rd insert of value 2 on (page 2, entry 2)
         *   4th insert of value 0 on (page 0, entry 4) ...
         * Expect page number and key number to be the same.
         */
        for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
            bp.insertKey(new IntDataBox(i % 3), new RecordID(i % 3, i));
        }
        Iterator<RecordID> rids = bp.sortedScan();
        assertTrue(rids.hasNext());

        for (int i = 0; i < intLeafPageSize + 1; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
            assertEquals(i * 3, rid.getEntryNumber());
        }

        for (int i = 0; i < intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(1, rid.getPageNum());
        }

        for (int i = 0; i < intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(2, rid.getPageNum());
        }
        assertFalse(rids.hasNext());
    }

    @Test
    public void testBPlusTreeSweepInsertLookupKeyMultipleFullLeafSplit() {
        /**
         * Insert four full leaves of records in a sweeping fashion.
         * Ensure that you correctly handle multiple leaf splits.
         */
        for (int i = 0; i < 8*intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 4), new RecordID(i % 4, i));
        }
        Iterator<RecordID> rids;

        rids = bp.lookupKey(new IntDataBox(0));
        System.out.println(rids);
        assertTrue(rids.hasNext());

        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue("iteration " + i, rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        rids = bp.lookupKey(new IntDataBox(1));
        assertTrue(rids.hasNext());
        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(1, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        rids = bp.lookupKey(new IntDataBox(2));
        assertTrue(rids.hasNext());

        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(2, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        rids = bp.lookupKey(new IntDataBox(3));
        assertTrue(rids.hasNext());

        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(3, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

    }

    @Test
    public void testBPlusTreeSweepInsertSortedScanLeafSplit() {
        /**
         * Insert ten full leaves of records in a sweeping fashion.
         * Ensure that iterator works when a value spans more than one page.
         * Example: Two leaf pages will contain keys with a value of 0.
         */
        for (int i = 0; i < 10*intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 5), new RecordID(i % 5, i));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        assertTrue(rids.hasNext());
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 2*intLeafPageSize; j++) {
                assertTrue(rids.hasNext());
                RecordID rid = rids.next();
                assertEquals(i, rid.getPageNum());
            }
        }
        assertFalse(rids.hasNext());

    }

    @Test
    public void testBPlusTreeSweepInsertSortedScanFromLeafSplit() {
        /**
         * Insert ten full leaves of records in a sweeping fashion.
         * Ensure that sortedScanFrom works when a value spans more than one
         * page.
         */
        for (int i = 0; i < 10*intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 5), new RecordID(i % 5, i));
        }
        for (int k = 0; k < 5; k++) {
            Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataBox(k));
            assertTrue(rids.hasNext());
            for (int i = k; i < 5; i++) {
                for (int j = 0; j < 2*intLeafPageSize; j++) {
                    assertTrue(rids.hasNext());
                    RecordID rid = rids.next();
                    assertEquals(i, rid.getPageNum());
                }
            }
            assertFalse(rids.hasNext());
        }
    }

    @Test
    public void testBPlusTreeAppendInsertSortedScanInnerSplit() {
        /**
         * Insert enough keys to cause an InnerNode split.
         */
        for (int i = 0; i < (intInnPageSize/2 + 1)*(intLeafPageSize); i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i, 0));
        }
        Iterator<RecordID> rids = bp.sortedScan();

        for (int i = 0; i < (intInnPageSize/2 + 1)*(intLeafPageSize); i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(i, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

    }

    @Test
    public void testBPlusTreeSweepInsertLookupInnerSplit() {
        /**
         * Insert enough keys to cause an InnerNode split: numEntries +
         * firstChild.
         * Each key should span 2 pages.
         */
        for (int i = 0; i < 2*intLeafPageSize; i++) {
            for (int k = 0; k < 250; k++) {
                bp.insertKey(new IntDataBox(k), new RecordID(k, 0));
            }
        }

        for (int k = 0; k < 250; k++) {
            Iterator<RecordID> rids = bp.lookupKey(new IntDataBox(k));
            for (int i = 0; i < 2*intLeafPageSize; i++) {
                assertTrue("Loop: " + k + " iteration " + i, rids.hasNext());
                RecordID rid = rids.next();
                assertEquals(k, rid.getPageNum());
            }
            assertFalse(rids.hasNext());
        }
    }
}
