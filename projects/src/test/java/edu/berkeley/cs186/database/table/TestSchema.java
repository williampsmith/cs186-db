package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.databox.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestSchema {
  @Test
  public void testSchemaRetrieve() {
    Schema schema = TestUtils.createSchemaWithAllTypes();

    Record input = TestUtils.createRecordWithAllTypes();
    byte[] encoded = schema.encode(input);
    Record decoded = schema.decode(encoded);

    assertEquals(input, decoded);
  }

  @Test
  public void testValidRecord() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    try {
      Record output = schema.verify(input.getValues());
      assertEquals(input, output);
    } catch (SchemaException se) {
      fail();
    }
  }


  @Test(expected = SchemaException.class)
  public void testInvalidRecordLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    schema.verify(new ArrayList<DataBox>());
  }

  @Test(expected = SchemaException.class)
  public void testInvalidFields() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();

    values.add(new StringDataBox("abcde", 5));
    values.add(new IntDataBox(10));

    schema.verify(values);
  }

  /**
   ***************************************
   * Beginning of student added test cases
   ***************************************
   */

  @Test(expected = SchemaException.class)
  @Category(StudentTest.class)
  public void testShortFieldLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();

    values.add(new BoolDataBox(true));
    values.add(new IntDataBox(10));
    values.add(new StringDataBox("abcd", 4));
    values.add(new FloatDataBox(10));

    schema.verify(values);
  }

  @Test(expected = SchemaException.class)
  @Category(StudentTest.class)
  public void testLongFieldLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();

    values.add(new BoolDataBox(true));
    values.add(new IntDataBox(10));
    values.add(new StringDataBox("abcdef", 6));
    values.add(new FloatDataBox(10));

    schema.verify(values);
  }

  @Test
  @Category(StudentTest.class)
  public void testCorrectFields() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();

    values.add(new BoolDataBox(true));
    values.add(new IntDataBox(10));
    values.add(new StringDataBox("abcde", 5));
    values.add(new FloatDataBox(10));

    schema.verify(values);
  }

  @Test(expected = SchemaException.class)
  @Category(StudentTest.class)
  public void testPermutedFields() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();

    values.add(new IntDataBox(10));
    values.add(new BoolDataBox(true));
    values.add(new FloatDataBox(10));
    values.add(new StringDataBox("abcde", 5));

    schema.verify(values);
  }

}
