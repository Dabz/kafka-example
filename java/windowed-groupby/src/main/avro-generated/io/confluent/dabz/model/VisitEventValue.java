/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package io.confluent.dabz.model;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class VisitEventValue extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 7589800581138342986L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"VisitEventValue\",\"namespace\":\"io.confluent.dabz.model\",\"fields\":[{\"name\":\"user\",\"type\":\"string\",\"doc\":\"username that visited the page\"},{\"name\":\"time\",\"type\":\"int\",\"doc\":\"time of the visits\",\"logicalType\":\"time-millis\"},{\"name\":\"timeSpent\",\"type\":\"long\",\"doc\":\"duration of the user's visit\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<VisitEventValue> ENCODER =
      new BinaryMessageEncoder<VisitEventValue>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<VisitEventValue> DECODER =
      new BinaryMessageDecoder<VisitEventValue>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<VisitEventValue> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<VisitEventValue> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<VisitEventValue>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this VisitEventValue to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a VisitEventValue from a ByteBuffer. */
  public static VisitEventValue fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  /** username that visited the page */
  @Deprecated public java.lang.CharSequence user;
  /** time of the visits */
  @Deprecated public int time;
  /** duration of the user's visit */
  @Deprecated public long timeSpent;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public VisitEventValue() {}

  /**
   * All-args constructor.
   * @param user username that visited the page
   * @param time time of the visits
   * @param timeSpent duration of the user's visit
   */
  public VisitEventValue(java.lang.CharSequence user, java.lang.Integer time, java.lang.Long timeSpent) {
    this.user = user;
    this.time = time;
    this.timeSpent = timeSpent;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return user;
    case 1: return time;
    case 2: return timeSpent;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: user = (java.lang.CharSequence)value$; break;
    case 1: time = (java.lang.Integer)value$; break;
    case 2: timeSpent = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'user' field.
   * @return username that visited the page
   */
  public java.lang.CharSequence getUser() {
    return user;
  }

  /**
   * Sets the value of the 'user' field.
   * username that visited the page
   * @param value the value to set.
   */
  public void setUser(java.lang.CharSequence value) {
    this.user = value;
  }

  /**
   * Gets the value of the 'time' field.
   * @return time of the visits
   */
  public java.lang.Integer getTime() {
    return time;
  }

  /**
   * Sets the value of the 'time' field.
   * time of the visits
   * @param value the value to set.
   */
  public void setTime(java.lang.Integer value) {
    this.time = value;
  }

  /**
   * Gets the value of the 'timeSpent' field.
   * @return duration of the user's visit
   */
  public java.lang.Long getTimeSpent() {
    return timeSpent;
  }

  /**
   * Sets the value of the 'timeSpent' field.
   * duration of the user's visit
   * @param value the value to set.
   */
  public void setTimeSpent(java.lang.Long value) {
    this.timeSpent = value;
  }

  /**
   * Creates a new VisitEventValue RecordBuilder.
   * @return A new VisitEventValue RecordBuilder
   */
  public static io.confluent.dabz.model.VisitEventValue.Builder newBuilder() {
    return new io.confluent.dabz.model.VisitEventValue.Builder();
  }

  /**
   * Creates a new VisitEventValue RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new VisitEventValue RecordBuilder
   */
  public static io.confluent.dabz.model.VisitEventValue.Builder newBuilder(io.confluent.dabz.model.VisitEventValue.Builder other) {
    return new io.confluent.dabz.model.VisitEventValue.Builder(other);
  }

  /**
   * Creates a new VisitEventValue RecordBuilder by copying an existing VisitEventValue instance.
   * @param other The existing instance to copy.
   * @return A new VisitEventValue RecordBuilder
   */
  public static io.confluent.dabz.model.VisitEventValue.Builder newBuilder(io.confluent.dabz.model.VisitEventValue other) {
    return new io.confluent.dabz.model.VisitEventValue.Builder(other);
  }

  /**
   * RecordBuilder for VisitEventValue instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<VisitEventValue>
    implements org.apache.avro.data.RecordBuilder<VisitEventValue> {

    /** username that visited the page */
    private java.lang.CharSequence user;
    /** time of the visits */
    private int time;
    /** duration of the user's visit */
    private long timeSpent;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(io.confluent.dabz.model.VisitEventValue.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.user)) {
        this.user = data().deepCopy(fields()[0].schema(), other.user);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.time)) {
        this.time = data().deepCopy(fields()[1].schema(), other.time);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.timeSpent)) {
        this.timeSpent = data().deepCopy(fields()[2].schema(), other.timeSpent);
        fieldSetFlags()[2] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing VisitEventValue instance
     * @param other The existing instance to copy.
     */
    private Builder(io.confluent.dabz.model.VisitEventValue other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.user)) {
        this.user = data().deepCopy(fields()[0].schema(), other.user);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.time)) {
        this.time = data().deepCopy(fields()[1].schema(), other.time);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.timeSpent)) {
        this.timeSpent = data().deepCopy(fields()[2].schema(), other.timeSpent);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'user' field.
      * username that visited the page
      * @return The value.
      */
    public java.lang.CharSequence getUser() {
      return user;
    }

    /**
      * Sets the value of the 'user' field.
      * username that visited the page
      * @param value The value of 'user'.
      * @return This builder.
      */
    public io.confluent.dabz.model.VisitEventValue.Builder setUser(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.user = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'user' field has been set.
      * username that visited the page
      * @return True if the 'user' field has been set, false otherwise.
      */
    public boolean hasUser() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'user' field.
      * username that visited the page
      * @return This builder.
      */
    public io.confluent.dabz.model.VisitEventValue.Builder clearUser() {
      user = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'time' field.
      * time of the visits
      * @return The value.
      */
    public java.lang.Integer getTime() {
      return time;
    }

    /**
      * Sets the value of the 'time' field.
      * time of the visits
      * @param value The value of 'time'.
      * @return This builder.
      */
    public io.confluent.dabz.model.VisitEventValue.Builder setTime(int value) {
      validate(fields()[1], value);
      this.time = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'time' field has been set.
      * time of the visits
      * @return True if the 'time' field has been set, false otherwise.
      */
    public boolean hasTime() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'time' field.
      * time of the visits
      * @return This builder.
      */
    public io.confluent.dabz.model.VisitEventValue.Builder clearTime() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'timeSpent' field.
      * duration of the user's visit
      * @return The value.
      */
    public java.lang.Long getTimeSpent() {
      return timeSpent;
    }

    /**
      * Sets the value of the 'timeSpent' field.
      * duration of the user's visit
      * @param value The value of 'timeSpent'.
      * @return This builder.
      */
    public io.confluent.dabz.model.VisitEventValue.Builder setTimeSpent(long value) {
      validate(fields()[2], value);
      this.timeSpent = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'timeSpent' field has been set.
      * duration of the user's visit
      * @return True if the 'timeSpent' field has been set, false otherwise.
      */
    public boolean hasTimeSpent() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'timeSpent' field.
      * duration of the user's visit
      * @return This builder.
      */
    public io.confluent.dabz.model.VisitEventValue.Builder clearTimeSpent() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public VisitEventValue build() {
      try {
        VisitEventValue record = new VisitEventValue();
        record.user = fieldSetFlags()[0] ? this.user : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.time = fieldSetFlags()[1] ? this.time : (java.lang.Integer) defaultValue(fields()[1]);
        record.timeSpent = fieldSetFlags()[2] ? this.timeSpent : (java.lang.Long) defaultValue(fields()[2]);
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<VisitEventValue>
    WRITER$ = (org.apache.avro.io.DatumWriter<VisitEventValue>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<VisitEventValue>
    READER$ = (org.apache.avro.io.DatumReader<VisitEventValue>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
