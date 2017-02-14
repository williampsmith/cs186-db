package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.io.Page;
import sun.nio.cs.StandardCharsets;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.*;
import java.io.ByteArrayInputStream;

/**
 * The Schema of a particular table.
 *
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a record conforming to this schema
 */
public class Schema {
  private List<String> fields;
  private List<DataBox> fieldTypes;
  private int size;

  public Schema(List<String> fields, List<DataBox> fieldTypes) {
    assert(fields.size() == fieldTypes.size());

    this.fields = fields;
    this.fieldTypes = fieldTypes;
    this.size = 0;


    for (DataBox dt : fieldTypes) {
      this.size += dt.getSize();
    }
  }

  /**
   * Verifies that a list of DataBoxes corresponds to this schema. A list of
   * DataBoxes corresponds to this schema if the number of DataBoxes in the
   * list equals the number of columns in this schema, and if each DataBox has
   * the same type and size as the columns in this schema.
   *
   * @param values the list of values to check
   * @return a new Record with the DataBoxes specified
   * @throws SchemaException if the values specified don't conform to this Schema
   */
  public Record verify(List<DataBox> values) throws SchemaException {
    if (this.fieldTypes.size() != values.size()) {
      throw new SchemaException("Size of record does not match size of schema for this table");
    }

    for (int i = 0; i < values.size(); i++) {
      if (values.get(i).type() != this.fieldTypes.get(i).type() ||
              values.get(i).getSize() != this.fieldTypes.get(i).getSize()) {
        throw new SchemaException("Values do not match schema.");
      }
    }

    return new Record(values);
  }

  // addon
  public int getSize() { return this.size; }

  /**
   * Serializes the provided record into a byte[]. Uses the DataBoxes'
   * serialization methods. A serialized record is represented as the
   * concatenation of each serialized DataBox. This method assumes that the
   * input record corresponds to this schema.
   *
   * @param record the record to encode
   * @return the encoded record as a byte[]
   */
  public byte[] encode(Record record) {
    // get size of concatenated array
    int totalLen = 0;
    for (DataBox value : record.getValues()) {
      totalLen += value.getSize();
    }

    ByteBuffer serialization = ByteBuffer.allocate(totalLen);
    for (DataBox value : record.getValues()) {
      byte[] serializedValue = value.getBytes();
      serialization.put(serializedValue);
    }

    return serialization.array();
  }

  /**
   * Takes a byte[] and decodes it into a Record. This method assumes that the
   * input byte[] represents a record that corresponds to this schema.
   *
   * @param input the byte array to decode
   * @return the decoded Record
   */
  public Record decode(byte[] input) {
    // TODO: implement me!
    int i = 0;
    List<DataBox> values = new ArrayList<DataBox>();
    ByteArrayInputStream bytes;
    ObjectInputStream obj;

    for (DataBox dataBox : this.fieldTypes) {
      byte[] subArray = Arrays.copyOfRange(input, i, i + dataBox.getSize());
      i += dataBox.getSize();

      DataBox.Types type = dataBox.type();

      DataBox field;

      switch(type) {
        case INT:
          field = new IntDataBox(subArray);
          break;
        case STRING:

          field = new StringDataBox(subArray);
          break;
        case BOOL:
          field = new BoolDataBox(subArray);
          break;
        case FLOAT:
        default:
          field = new FloatDataBox(subArray);
          break;
      }

      values.add(field);
    }

    try {
      return this.verify(values);
    } catch (Exception e){
      System.out.println("Error. schema fails verification " + e.getMessage());
      return null;
    }
  }

  public int getEntrySize() {
    return this.size;
  }

  public List<String> getFieldNames() {
    return this.fields;
  }

  public List<DataBox> getFieldTypes() {
    return this.fieldTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Schema)) {
      return false;
    }

    Schema otherSchema = (Schema) other;

    if (this.fields.size() != otherSchema.fields.size()) {
      return false;
    }

    for (int i = 0; i < this.fields.size(); i++) {
      DataBox thisType = this.fieldTypes.get(i);
      DataBox otherType = otherSchema.fieldTypes.get(i);

      if (thisType.type() != otherType.type()) {
        return false;
      }

      if (thisType.type().equals(DataBox.Types.STRING) && thisType.getSize() != otherType.getSize()) {
        return false;
      }
    }

    return true;
  }
}