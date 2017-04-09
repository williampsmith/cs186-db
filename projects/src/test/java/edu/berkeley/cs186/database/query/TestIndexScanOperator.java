package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.io.Page;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP3;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.rules.TemporaryFolder;


import javax.management.Query;

import static org.junit.Assert.*;

public class TestIndexScanOperator {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test(timeout=5000)
    public void testIndexScanEqualsRecords() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

        Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
        List<DataBox> r1Vals = r1.getValues();
        Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
        List<DataBox> r2Vals = r2.getValues();
        Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
        List<DataBox> r3Vals = r3.getValues();
        Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
        List<DataBox> r4Vals = r4.getValues();
        Record r5 = TestUtils.createRecordWithAllTypesWithValue(5);
        List<DataBox> r5Vals = r5.getValues();
        Record r6 = TestUtils.createRecordWithAllTypesWithValue(6);
        List<DataBox> r6Vals = r6.getValues();
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord3 = new Record(expectedRecordValues3);
        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);

        for (int i = 0; i < 99; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;

        while (outputIterator.hasNext()) {
            if (count < 99) {
                assertEquals(expectedRecord3, outputIterator.next());
            }
            count++;
        }
        assertTrue(count == 99);
    }

    @Test(expected=QueryPlanException.class)
    @Category(StudentTestP3.class)
    public void testInvalidIndex() throws IOException, DatabaseException, QueryPlanException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

        IndexScanOperator operator = new IndexScanOperator(transaction,
                "Nonexistent Table",
                "Nonexistent Column",
                QueryPlan.PredicateOperator.EQUALS,
                new IntDataBox(0));
    }


    @Test(timeout=6000)
    @Category(StudentTestP3.class)
    public void testIndexScanGreaterThanEqualsRecords() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

        Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
        List<DataBox> r1Vals = r1.getValues();
        Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
        List<DataBox> r2Vals = r2.getValues();
        Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
        List<DataBox> r3Vals = r3.getValues();
        Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
        List<DataBox> r4Vals = r4.getValues();
        Record r5 = TestUtils.createRecordWithAllTypesWithValue(5);
        List<DataBox> r5Vals = r5.getValues();
        Record r6 = TestUtils.createRecordWithAllTypesWithValue(6);
        List<DataBox> r6Vals = r6.getValues();
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord4 = new Record(expectedRecordValues4);
        Record expectedRecord5 = new Record(expectedRecordValues5);
        Record expectedRecord6 = new Record(expectedRecordValues6);

        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);

        for (int i = 0; i < 99; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
            transaction.addRecord("myTable", r4Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(4));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 99; j++) {
                assertTrue(outputIterator.hasNext());
                Record next = outputIterator.next();
                if (i == 0) {
                    assertEquals(expectedRecord4, next);
                } else if (i == 1) {
                    assertEquals(expectedRecord5, next);
                } else {
                    assertEquals(expectedRecord6, next);
                }
                count++;
            }
        }
        assertTrue(count == (3*99));
    }


    @Test(timeout=6000)
    @Category(StudentTestP3.class)
    public void testIndexScanGreaterThanRecords() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

        Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
        List<DataBox> r1Vals = r1.getValues();
        Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
        List<DataBox> r2Vals = r2.getValues();
        Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
        List<DataBox> r3Vals = r3.getValues();
        Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
        List<DataBox> r4Vals = r4.getValues();
        Record r5 = TestUtils.createRecordWithAllTypesWithValue(5);
        List<DataBox> r5Vals = r5.getValues();
        Record r6 = TestUtils.createRecordWithAllTypesWithValue(6);
        List<DataBox> r6Vals = r6.getValues();
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord4 = new Record(expectedRecordValues4);
        Record expectedRecord5 = new Record(expectedRecordValues5);
        Record expectedRecord6 = new Record(expectedRecordValues6);

        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);

        for (int i = 0; i < 99; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
            transaction.addRecord("myTable", r4Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 99; j++) {
                assertTrue(outputIterator.hasNext());
                Record next = outputIterator.next();
                if (i == 0) {
                    assertEquals(expectedRecord4, next);
                } else if (i == 1) {
                    assertEquals(expectedRecord5, next);
                } else {
                    assertEquals(expectedRecord6, next);
                }
                count++;
            }
        }
        assertTrue(count == (3*99));
    }

    @Test(timeout=6000)
    @Category(StudentTestP3.class)
    public void testIndexScanLessThanEqualsRecords() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

        Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
        List<DataBox> r1Vals = r1.getValues();
        Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
        List<DataBox> r2Vals = r2.getValues();
        Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
        List<DataBox> r3Vals = r3.getValues();
        Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
        List<DataBox> r4Vals = r4.getValues();
        Record r5 = TestUtils.createRecordWithAllTypesWithValue(5);
        List<DataBox> r5Vals = r5.getValues();
        Record r6 = TestUtils.createRecordWithAllTypesWithValue(6);
        List<DataBox> r6Vals = r6.getValues();
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        Record expectedRecord3 = new Record(expectedRecordValues3);

        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);

        for (int i = 0; i < 99; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
            transaction.addRecord("myTable", r4Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.LESS_THAN_EQUALS, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 99; j++) {
                assertTrue(outputIterator.hasNext());
                Record next = outputIterator.next();
                if (i == 0) {
                    assertEquals(expectedRecord1, next);
                } else if (i == 1) {
                    assertEquals(expectedRecord2, next);
                } else {
                    assertEquals(expectedRecord3, next);
                }
                count++;
            }
        }
        assertTrue(count == (3*99));
    }

    @Test(timeout=6000)
    @Category(StudentTestP3.class)
    public void testIndexScanLessThanRecords() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

        Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
        List<DataBox> r1Vals = r1.getValues();
        Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
        List<DataBox> r2Vals = r2.getValues();
        Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
        List<DataBox> r3Vals = r3.getValues();
        Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
        List<DataBox> r4Vals = r4.getValues();
        Record r5 = TestUtils.createRecordWithAllTypesWithValue(5);
        List<DataBox> r5Vals = r5.getValues();
        Record r6 = TestUtils.createRecordWithAllTypesWithValue(6);
        List<DataBox> r6Vals = r6.getValues();
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        Record expectedRecord3 = new Record(expectedRecordValues3);

        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);

        for (int i = 0; i < 99; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
            transaction.addRecord("myTable", r4Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(4));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 99; j++) {
                assertTrue(outputIterator.hasNext());
                Record next = outputIterator.next();
                if (i == 0) {
                    assertEquals(expectedRecord1, next);
                } else if (i == 1) {
                    assertEquals(expectedRecord2, next);
                } else {
                    assertEquals(expectedRecord3, next);
                }
                count++;
            }
        }
        assertTrue(count == (3*99));
    }

    @Test(timeout=6000)
    @Category(StudentTestP3.class)
    public void testIndexScanNotEqualsRecords() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

        Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
        List<DataBox> r1Vals = r1.getValues();
        Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
        List<DataBox> r2Vals = r2.getValues();
        Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
        List<DataBox> r3Vals = r3.getValues();
        Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
        List<DataBox> r4Vals = r4.getValues();
        Record r5 = TestUtils.createRecordWithAllTypesWithValue(5);
        List<DataBox> r5Vals = r5.getValues();
        Record r6 = TestUtils.createRecordWithAllTypesWithValue(6);
        List<DataBox> r6Vals = r6.getValues();
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        Record expectedRecord3 = new Record(expectedRecordValues3);
        Record expectedRecord5 = new Record(expectedRecordValues5);
        Record expectedRecord6 = new Record(expectedRecordValues6);

        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);

        for (int i = 0; i < 99; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
            transaction.addRecord("myTable", r4Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.NOT_EQUALS, new IntDataBox(4));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 99; j++) {
                assertTrue(outputIterator.hasNext());
                Record next = outputIterator.next();
                if (i == 0) {
                    assertEquals(expectedRecord1, next);
                } else if (i == 1) {
                    assertEquals(expectedRecord2, next);
                } else if (i == 2) {
                    assertEquals(expectedRecord3, next);
                } else if (i == 3) {
                    assertEquals(expectedRecord5, next);
                } else if (i == 4) {
                    assertEquals(expectedRecord6, next);
                }
                count++;
            }
        }
        assertTrue(count == (5*99));
    }

    @Test(timeout=6000)
    @Category(StudentTestP3.class)
    public void testEmptyResults() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

        Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
        List<DataBox> r1Vals = r1.getValues();
        Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
        List<DataBox> r2Vals = r2.getValues();
        Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
        List<DataBox> r3Vals = r3.getValues();
        Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
        List<DataBox> r4Vals = r4.getValues();
        Record r5 = TestUtils.createRecordWithAllTypesWithValue(5);
        List<DataBox> r5Vals = r5.getValues();
        Record r6 = TestUtils.createRecordWithAllTypesWithValue(6);
        List<DataBox> r6Vals = r6.getValues();
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        Record expectedRecord3 = new Record(expectedRecordValues3);

        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);

        for (int i = 0; i < 99; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
            transaction.addRecord("myTable", r4Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(1));
        Iterator<Record> outputLessThanIterator = s1.iterator();
        assertFalse(outputLessThanIterator.hasNext());

        QueryOperator s2 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(6));
        Iterator<Record> outputGreaterThanIterator = s2.iterator();
        assertFalse(outputGreaterThanIterator.hasNext());

        QueryOperator s3 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(7));
        Iterator<Record> outputGreaterThanEqualsIterator = s3.iterator();
        assertFalse(outputGreaterThanEqualsIterator.hasNext());
    }
}