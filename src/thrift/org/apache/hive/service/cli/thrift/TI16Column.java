/**
 * Autogenerated by Thrift Compiler (0.14.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hive.service.cli.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.0)", date = "2022-01-05")
public class TI16Column implements org.apache.thrift.TBase<TI16Column, TI16Column._Fields>, java.io.Serializable, Cloneable, Comparable<TI16Column> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TI16Column");

  private static final org.apache.thrift.protocol.TField VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("values", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField NULLS_FIELD_DESC = new org.apache.thrift.protocol.TField("nulls", org.apache.thrift.protocol.TType.STRING, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TI16ColumnStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TI16ColumnTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.util.List<java.lang.Short> values; // required
  public @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer nulls; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    VALUES((short)1, "values"),
    NULLS((short)2, "nulls");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // VALUES
          return VALUES;
        case 2: // NULLS
          return NULLS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.VALUES, new org.apache.thrift.meta_data.FieldMetaData("values", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I16))));
    tmpMap.put(_Fields.NULLS, new org.apache.thrift.meta_data.FieldMetaData("nulls", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TI16Column.class, metaDataMap);
  }

  public TI16Column() {
  }

  public TI16Column(
    java.util.List<java.lang.Short> values,
    java.nio.ByteBuffer nulls)
  {
    this();
    this.values = values;
    this.nulls = org.apache.thrift.TBaseHelper.copyBinary(nulls);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TI16Column(TI16Column other) {
    if (other.isSetValues()) {
      java.util.List<java.lang.Short> __this__values = new java.util.ArrayList<java.lang.Short>(other.values);
      this.values = __this__values;
    }
    if (other.isSetNulls()) {
      this.nulls = org.apache.thrift.TBaseHelper.copyBinary(other.nulls);
    }
  }

  public TI16Column deepCopy() {
    return new TI16Column(this);
  }

  @Override
  public void clear() {
    this.values = null;
    this.nulls = null;
  }

  public int getValuesSize() {
    return (this.values == null) ? 0 : this.values.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.lang.Short> getValuesIterator() {
    return (this.values == null) ? null : this.values.iterator();
  }

  public void addToValues(short elem) {
    if (this.values == null) {
      this.values = new java.util.ArrayList<java.lang.Short>();
    }
    this.values.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.lang.Short> getValues() {
    return this.values;
  }

  public TI16Column setValues(@org.apache.thrift.annotation.Nullable java.util.List<java.lang.Short> values) {
    this.values = values;
    return this;
  }

  public void unsetValues() {
    this.values = null;
  }

  /** Returns true if field values is set (has been assigned a value) and false otherwise */
  public boolean isSetValues() {
    return this.values != null;
  }

  public void setValuesIsSet(boolean value) {
    if (!value) {
      this.values = null;
    }
  }

  public byte[] getNulls() {
    setNulls(org.apache.thrift.TBaseHelper.rightSize(nulls));
    return nulls == null ? null : nulls.array();
  }

  public java.nio.ByteBuffer bufferForNulls() {
    return org.apache.thrift.TBaseHelper.copyBinary(nulls);
  }

  public TI16Column setNulls(byte[] nulls) {
    this.nulls = nulls == null ? (java.nio.ByteBuffer)null   : java.nio.ByteBuffer.wrap(nulls.clone());
    return this;
  }

  public TI16Column setNulls(@org.apache.thrift.annotation.Nullable java.nio.ByteBuffer nulls) {
    this.nulls = org.apache.thrift.TBaseHelper.copyBinary(nulls);
    return this;
  }

  public void unsetNulls() {
    this.nulls = null;
  }

  /** Returns true if field nulls is set (has been assigned a value) and false otherwise */
  public boolean isSetNulls() {
    return this.nulls != null;
  }

  public void setNullsIsSet(boolean value) {
    if (!value) {
      this.nulls = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case VALUES:
      if (value == null) {
        unsetValues();
      } else {
        setValues((java.util.List<java.lang.Short>)value);
      }
      break;

    case NULLS:
      if (value == null) {
        unsetNulls();
      } else {
        if (value instanceof byte[]) {
          setNulls((byte[])value);
        } else {
          setNulls((java.nio.ByteBuffer)value);
        }
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case VALUES:
      return getValues();

    case NULLS:
      return getNulls();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case VALUES:
      return isSetValues();
    case NULLS:
      return isSetNulls();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TI16Column)
      return this.equals((TI16Column)that);
    return false;
  }

  public boolean equals(TI16Column that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_values = true && this.isSetValues();
    boolean that_present_values = true && that.isSetValues();
    if (this_present_values || that_present_values) {
      if (!(this_present_values && that_present_values))
        return false;
      if (!this.values.equals(that.values))
        return false;
    }

    boolean this_present_nulls = true && this.isSetNulls();
    boolean that_present_nulls = true && that.isSetNulls();
    if (this_present_nulls || that_present_nulls) {
      if (!(this_present_nulls && that_present_nulls))
        return false;
      if (!this.nulls.equals(that.nulls))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetValues()) ? 131071 : 524287);
    if (isSetValues())
      hashCode = hashCode * 8191 + values.hashCode();

    hashCode = hashCode * 8191 + ((isSetNulls()) ? 131071 : 524287);
    if (isSetNulls())
      hashCode = hashCode * 8191 + nulls.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TI16Column other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetValues(), other.isSetValues());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValues()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.values, other.values);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetNulls(), other.isSetNulls());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNulls()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.nulls, other.nulls);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TI16Column(");
    boolean first = true;

    sb.append("values:");
    if (this.values == null) {
      sb.append("null");
    } else {
      sb.append(this.values);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("nulls:");
    if (this.nulls == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.nulls, sb);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (values == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'values' was not present! Struct: " + toString());
    }
    if (nulls == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'nulls' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TI16ColumnStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TI16ColumnStandardScheme getScheme() {
      return new TI16ColumnStandardScheme();
    }
  }

  private static class TI16ColumnStandardScheme extends org.apache.thrift.scheme.StandardScheme<TI16Column> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TI16Column struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // VALUES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list70 = iprot.readListBegin();
                struct.values = new java.util.ArrayList<java.lang.Short>(_list70.size);
                short _elem71;
                for (int _i72 = 0; _i72 < _list70.size; ++_i72)
                {
                  _elem71 = iprot.readI16();
                  struct.values.add(_elem71);
                }
                iprot.readListEnd();
              }
              struct.setValuesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // NULLS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.nulls = iprot.readBinary();
              struct.setNullsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TI16Column struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.values != null) {
        oprot.writeFieldBegin(VALUES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I16, struct.values.size()));
          for (short _iter73 : struct.values)
          {
            oprot.writeI16(_iter73);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.nulls != null) {
        oprot.writeFieldBegin(NULLS_FIELD_DESC);
        oprot.writeBinary(struct.nulls);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TI16ColumnTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TI16ColumnTupleScheme getScheme() {
      return new TI16ColumnTupleScheme();
    }
  }

  private static class TI16ColumnTupleScheme extends org.apache.thrift.scheme.TupleScheme<TI16Column> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TI16Column struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        oprot.writeI32(struct.values.size());
        for (short _iter74 : struct.values)
        {
          oprot.writeI16(_iter74);
        }
      }
      oprot.writeBinary(struct.nulls);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TI16Column struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list75 = iprot.readListBegin(org.apache.thrift.protocol.TType.I16);
        struct.values = new java.util.ArrayList<java.lang.Short>(_list75.size);
        short _elem76;
        for (int _i77 = 0; _i77 < _list75.size; ++_i77)
        {
          _elem76 = iprot.readI16();
          struct.values.add(_elem76);
        }
      }
      struct.setValuesIsSet(true);
      struct.nulls = iprot.readBinary();
      struct.setNullsIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

