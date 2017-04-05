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
}