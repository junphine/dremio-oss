package com.dremio.exec.store.jdbc.proto;

import java.nio.*;
import java.io.*;
import java.util.*;
import com.google.protobuf.*;

public final class JdbcReaderProto
{
    private static final Descriptors.Descriptor internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor;
    private static final GeneratedMessageV3.FieldAccessorTable internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_fieldAccessorTable;
    private static final Descriptors.Descriptor internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor;
    private static final GeneratedMessageV3.FieldAccessorTable internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_fieldAccessorTable;
    private static final Descriptors.Descriptor internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor;
    private static final GeneratedMessageV3.FieldAccessorTable internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_fieldAccessorTable;
    private static Descriptors.FileDescriptor descriptor;
    
    public static void registerAllExtensions(final ExtensionRegistryLite registry) {
    }
    
    public static void registerAllExtensions(final ExtensionRegistry registry) {
        registerAllExtensions((ExtensionRegistryLite)registry);
    }
    
    public static Descriptors.FileDescriptor getDescriptor() {
        return JdbcReaderProto.descriptor;
    }
    
    static {
        final String[] descriptorData = { "\n\njdbc.proto\u0012 com.dremio.exec.store.jdbc.proto\",\n\u000eColumnProperty\u0012\u000b\n\u0003key\u0018\u0001 \u0001(\t\u0012\r\n\u0005value\u0018\u0002 \u0001(\t\"m\n\u0010ColumnProperties\u0012\u0013\n\u000bcolumn_name\u0018\u0001 \u0001(\t\u0012D\n\nproperties\u0018\u0002 \u0003(\u000b20.com.dremio.exec.store.jdbc.proto.ColumnProperty\"x\n\u000eJdbcTableXattr\u0012\u0017\n\u000fskipped_columns\u0018\u0001 \u0003(\t\u0012M\n\u0011column_properties\u0018\u0002 \u0003(\u000b22.com.dremio.exec.store.jdbc.proto.ColumnPropertiesB5\n com.dremio.exec.store.jdbc.protoB\u000fJdbcReaderProtoH\u0001" };
        JdbcReaderProto.descriptor = Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(descriptorData, new Descriptors.FileDescriptor[0]);
        internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor = getDescriptor().getMessageTypes().get(0);
        internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_fieldAccessorTable = new GeneratedMessageV3.FieldAccessorTable(JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor, new String[] { "Key", "Value" });
        internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor = getDescriptor().getMessageTypes().get(1);
        internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_fieldAccessorTable = new GeneratedMessageV3.FieldAccessorTable(JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor, new String[] { "ColumnName", "Properties" });
        internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor = getDescriptor().getMessageTypes().get(2);
        internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_fieldAccessorTable = new GeneratedMessageV3.FieldAccessorTable(JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor, new String[] { "SkippedColumns", "ColumnProperties" });
    }
    
    public static final class ColumnProperty extends GeneratedMessageV3 implements ColumnPropertyOrBuilder
    {
        private static final long serialVersionUID = 0L;
        private int bitField0_;
        public static final int KEY_FIELD_NUMBER = 1;
        private volatile Object key_;
        public static final int VALUE_FIELD_NUMBER = 2;
        private volatile Object value_;
        private byte memoizedIsInitialized;
        private static final ColumnProperty DEFAULT_INSTANCE;
        @Deprecated
        public static final Parser<ColumnProperty> PARSER;
        
        private ColumnProperty(final GeneratedMessageV3.Builder<?> builder) {
            super((GeneratedMessageV3.Builder)builder);
            this.memoizedIsInitialized = -1;
        }
        
        private ColumnProperty() {
            this.memoizedIsInitialized = -1;
            this.key_ = "";
            this.value_ = "";
        }
        
        protected Object newInstance(final GeneratedMessageV3.UnusedPrivateParameter unused) {
            return new ColumnProperty();
        }
        
        public final UnknownFieldSet getUnknownFields() {
            return this.unknownFields;
        }
        
        private ColumnProperty(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            this();
            if (extensionRegistry == null) {
                throw new NullPointerException();
            }
            final int mutable_bitField0_ = 0;
            final UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder();
            try {
                boolean done = false;
                while (!done) {
                    final int tag = input.readTag();
                    switch (tag) {
                        case 0: {
                            done = true;
                            continue;
                        }
                        case 10: {
                            final ByteString bs = input.readBytes();
                            this.bitField0_ |= 0x1;
                            this.key_ = bs;
                            continue;
                        }
                        case 18: {
                            final ByteString bs = input.readBytes();
                            this.bitField0_ |= 0x2;
                            this.value_ = bs;
                            continue;
                        }
                        default: {
                            if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                                done = true;
                                continue;
                            }
                            continue;
                        }
                    }
                }
            }
            catch (InvalidProtocolBufferException e) {
                throw e.setUnfinishedMessage((MessageLite)this);
            }
            catch (IOException e2) {
                throw new InvalidProtocolBufferException(e2).setUnfinishedMessage((MessageLite)this);
            }
            finally {
                this.unknownFields = unknownFields.build();
                this.makeExtensionsImmutable();
            }
        }
        
        public static final Descriptors.Descriptor getDescriptor() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor;
        }
        
        protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_fieldAccessorTable.ensureFieldAccessorsInitialized((Class)ColumnProperty.class, (Class)Builder.class);
        }
        
        public boolean hasKey() {
            return (this.bitField0_ & 0x1) != 0x0;
        }
        
        public String getKey() {
            final Object ref = this.key_;
            if (ref instanceof String) {
                return (String)ref;
            }
            final ByteString bs = (ByteString)ref;
            final String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
                this.key_ = s;
            }
            return s;
        }
        
        public ByteString getKeyBytes() {
            final Object ref = this.key_;
            if (ref instanceof String) {
                final ByteString b = ByteString.copyFromUtf8((String)ref);
                return (ByteString)(this.key_ = b);
            }
            return (ByteString)ref;
        }
        
        public boolean hasValue() {
            return (this.bitField0_ & 0x2) != 0x0;
        }
        
        public String getValue() {
            final Object ref = this.value_;
            if (ref instanceof String) {
                return (String)ref;
            }
            final ByteString bs = (ByteString)ref;
            final String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
                this.value_ = s;
            }
            return s;
        }
        
        public ByteString getValueBytes() {
            final Object ref = this.value_;
            if (ref instanceof String) {
                final ByteString b = ByteString.copyFromUtf8((String)ref);
                return (ByteString)(this.value_ = b);
            }
            return (ByteString)ref;
        }
        
        public final boolean isInitialized() {
            final byte isInitialized = this.memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }
            this.memoizedIsInitialized = 1;
            return true;
        }
        
        public void writeTo(final CodedOutputStream output) throws IOException {
            if ((this.bitField0_ & 0x1) != 0x0) {
                GeneratedMessageV3.writeString(output, 1, this.key_);
            }
            if ((this.bitField0_ & 0x2) != 0x0) {
                GeneratedMessageV3.writeString(output, 2, this.value_);
            }
            this.unknownFields.writeTo(output);
        }
        
        public int getSerializedSize() {
            int size = this.memoizedSize;
            if (size != -1) {
                return size;
            }
            size = 0;
            if ((this.bitField0_ & 0x1) != 0x0) {
                size += GeneratedMessageV3.computeStringSize(1, this.key_);
            }
            if ((this.bitField0_ & 0x2) != 0x0) {
                size += GeneratedMessageV3.computeStringSize(2, this.value_);
            }
            size += this.unknownFields.getSerializedSize();
            return this.memoizedSize = size;
        }
        
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof ColumnProperty)) {
                return super.equals(obj);
            }
            final ColumnProperty other = (ColumnProperty)obj;
            return this.hasKey() == other.hasKey() && (!this.hasKey() || this.getKey().equals(other.getKey())) && this.hasValue() == other.hasValue() && (!this.hasValue() || this.getValue().equals(other.getValue())) && this.unknownFields.equals(other.unknownFields);
        }
        
        public int hashCode() {
            if (this.memoizedHashCode != 0) {
                return this.memoizedHashCode;
            }
            int hash = 41;
            hash = 19 * hash + getDescriptor().hashCode();
            if (this.hasKey()) {
                hash = 37 * hash + 1;
                hash = 53 * hash + this.getKey().hashCode();
            }
            if (this.hasValue()) {
                hash = 37 * hash + 2;
                hash = 53 * hash + this.getValue().hashCode();
            }
            hash = 29 * hash + this.unknownFields.hashCode();
            return this.memoizedHashCode = hash;
        }
        
        public static ColumnProperty parseFrom(final ByteBuffer data) throws InvalidProtocolBufferException {
            return (ColumnProperty)ColumnProperty.PARSER.parseFrom(data);
        }
        
        public static ColumnProperty parseFrom(final ByteBuffer data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return (ColumnProperty)ColumnProperty.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static ColumnProperty parseFrom(final ByteString data) throws InvalidProtocolBufferException {
            return (ColumnProperty)ColumnProperty.PARSER.parseFrom(data);
        }
        
        public static ColumnProperty parseFrom(final ByteString data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return (ColumnProperty)ColumnProperty.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static ColumnProperty parseFrom(final byte[] data) throws InvalidProtocolBufferException {
            return (ColumnProperty)ColumnProperty.PARSER.parseFrom(data);
        }
        
        public static ColumnProperty parseFrom(final byte[] data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return (ColumnProperty)ColumnProperty.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static ColumnProperty parseFrom(final InputStream input) throws IOException {
            return (ColumnProperty)GeneratedMessageV3.parseWithIOException((Parser)ColumnProperty.PARSER, input);
        }
        
        public static ColumnProperty parseFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return (ColumnProperty)GeneratedMessageV3.parseWithIOException((Parser)ColumnProperty.PARSER, input, extensionRegistry);
        }
        
        public static ColumnProperty parseDelimitedFrom(final InputStream input) throws IOException {
            return (ColumnProperty)GeneratedMessageV3.parseDelimitedWithIOException((Parser)ColumnProperty.PARSER, input);
        }
        
        public static ColumnProperty parseDelimitedFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return (ColumnProperty)GeneratedMessageV3.parseDelimitedWithIOException((Parser)ColumnProperty.PARSER, input, extensionRegistry);
        }
        
        public static ColumnProperty parseFrom(final CodedInputStream input) throws IOException {
            return (ColumnProperty)GeneratedMessageV3.parseWithIOException((Parser)ColumnProperty.PARSER, input);
        }
        
        public static ColumnProperty parseFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return (ColumnProperty)GeneratedMessageV3.parseWithIOException((Parser)ColumnProperty.PARSER, input, extensionRegistry);
        }
        
        public Builder newBuilderForType() {
            return newBuilder();
        }
        
        public static Builder newBuilder() {
            return ColumnProperty.DEFAULT_INSTANCE.toBuilder();
        }
        
        public static Builder newBuilder(final ColumnProperty prototype) {
            return ColumnProperty.DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }
        
        public Builder toBuilder() {
            return (this == ColumnProperty.DEFAULT_INSTANCE) ? new Builder() : new Builder().mergeFrom(this);
        }
        
        protected Builder newBuilderForType(final GeneratedMessageV3.BuilderParent parent) {
            final Builder builder = new Builder(parent);
            return builder;
        }
        
        public static ColumnProperty getDefaultInstance() {
            return ColumnProperty.DEFAULT_INSTANCE;
        }
        
        public static Parser<ColumnProperty> parser() {
            return ColumnProperty.PARSER;
        }
        
        public Parser<ColumnProperty> getParserForType() {
            return ColumnProperty.PARSER;
        }
        
        public ColumnProperty getDefaultInstanceForType() {
            return ColumnProperty.DEFAULT_INSTANCE;
        }
        
        static {
            DEFAULT_INSTANCE = new ColumnProperty();
            PARSER = (Parser)new AbstractParser<ColumnProperty>() {
                public ColumnProperty parsePartialFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
                    return new ColumnProperty(input, extensionRegistry);
                }
            };
        }
        
        public static final class Builder extends GeneratedMessageV3.Builder<Builder> implements ColumnPropertyOrBuilder
        {
            private int bitField0_;
            private Object key_;
            private Object value_;
            
            public static final Descriptors.Descriptor getDescriptor() {
                return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor;
            }
            
            protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
                return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_fieldAccessorTable.ensureFieldAccessorsInitialized((Class)ColumnProperty.class, (Class)Builder.class);
            }
            
            private Builder() {
                this.key_ = "";
                this.value_ = "";
                this.maybeForceBuilderInitialization();
            }
            
            private Builder(final GeneratedMessageV3.BuilderParent parent) {
                super(parent);
                this.key_ = "";
                this.value_ = "";
                this.maybeForceBuilderInitialization();
            }
            
            private void maybeForceBuilderInitialization() {
                if (ColumnProperty.alwaysUseFieldBuilders) {}
            }
            
            public Builder clear() {
                super.clear();
                this.key_ = "";
                this.bitField0_ &= 0xFFFFFFFE;
                this.value_ = "";
                this.bitField0_ &= 0xFFFFFFFD;
                return this;
            }
            
            public Descriptors.Descriptor getDescriptorForType() {
                return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor;
            }
            
            public ColumnProperty getDefaultInstanceForType() {
                return ColumnProperty.getDefaultInstance();
            }
            
            public ColumnProperty build() {
                final ColumnProperty result = this.buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException((Message)result);
                }
                return result;
            }
            
            public ColumnProperty buildPartial() {
                final ColumnProperty result = new ColumnProperty((GeneratedMessageV3.Builder)this);
                final int from_bitField0_ = this.bitField0_;
                int to_bitField0_ = 0;
                if ((from_bitField0_ & 0x1) != 0x0) {
                    to_bitField0_ |= 0x1;
                }
                result.key_ = this.key_;
                if ((from_bitField0_ & 0x2) != 0x0) {
                    to_bitField0_ |= 0x2;
                }
                result.value_ = this.value_;
                result.bitField0_ = to_bitField0_;
                this.onBuilt();
                return result;
            }
            
            public Builder clone() {
                return (Builder)super.clone();
            }
            
            public Builder setField(final Descriptors.FieldDescriptor field, final Object value) {
                return (Builder)super.setField(field, value);
            }
            
            public Builder clearField(final Descriptors.FieldDescriptor field) {
                return (Builder)super.clearField(field);
            }
            
            public Builder clearOneof(final Descriptors.OneofDescriptor oneof) {
                return (Builder)super.clearOneof(oneof);
            }
            
            public Builder setRepeatedField(final Descriptors.FieldDescriptor field, final int index, final Object value) {
                return (Builder)super.setRepeatedField(field, index, value);
            }
            
            public Builder addRepeatedField(final Descriptors.FieldDescriptor field, final Object value) {
                return (Builder)super.addRepeatedField(field, value);
            }
            
            public Builder mergeFrom(final Message other) {
                if (other instanceof ColumnProperty) {
                    return this.mergeFrom((ColumnProperty)other);
                }
                super.mergeFrom(other);
                return this;
            }
            
            public Builder mergeFrom(final ColumnProperty other) {
                if (other == ColumnProperty.getDefaultInstance()) {
                    return this;
                }
                if (other.hasKey()) {
                    this.bitField0_ |= 0x1;
                    this.key_ = other.key_;
                    this.onChanged();
                }
                if (other.hasValue()) {
                    this.bitField0_ |= 0x2;
                    this.value_ = other.value_;
                    this.onChanged();
                }
                this.mergeUnknownFields(other.unknownFields);
                this.onChanged();
                return this;
            }
            
            public final boolean isInitialized() {
                return true;
            }
            
            public Builder mergeFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
                ColumnProperty parsedMessage = null;
                try {
                    parsedMessage = (ColumnProperty)ColumnProperty.PARSER.parsePartialFrom(input, extensionRegistry);
                }
                catch (InvalidProtocolBufferException e) {
                    parsedMessage = (ColumnProperty)e.getUnfinishedMessage();
                    throw e.unwrapIOException();
                }
                finally {
                    if (parsedMessage != null) {
                        this.mergeFrom(parsedMessage);
                    }
                }
                return this;
            }
            
            public boolean hasKey() {
                return (this.bitField0_ & 0x1) != 0x0;
            }
            
            public String getKey() {
                final Object ref = this.key_;
                if (!(ref instanceof String)) {
                    final ByteString bs = (ByteString)ref;
                    final String s = bs.toStringUtf8();
                    if (bs.isValidUtf8()) {
                        this.key_ = s;
                    }
                    return s;
                }
                return (String)ref;
            }
            
            public ByteString getKeyBytes() {
                final Object ref = this.key_;
                if (ref instanceof String) {
                    final ByteString b = ByteString.copyFromUtf8((String)ref);
                    return (ByteString)(this.key_ = b);
                }
                return (ByteString)ref;
            }
            
            public Builder setKey(final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x1;
                this.key_ = value;
                this.onChanged();
                return this;
            }
            
            public Builder clearKey() {
                this.bitField0_ &= 0xFFFFFFFE;
                this.key_ = ColumnProperty.getDefaultInstance().getKey();
                this.onChanged();
                return this;
            }
            
            public Builder setKeyBytes(final ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x1;
                this.key_ = value;
                this.onChanged();
                return this;
            }
            
            public boolean hasValue() {
                return (this.bitField0_ & 0x2) != 0x0;
            }
            
            public String getValue() {
                final Object ref = this.value_;
                if (!(ref instanceof String)) {
                    final ByteString bs = (ByteString)ref;
                    final String s = bs.toStringUtf8();
                    if (bs.isValidUtf8()) {
                        this.value_ = s;
                    }
                    return s;
                }
                return (String)ref;
            }
            
            public ByteString getValueBytes() {
                final Object ref = this.value_;
                if (ref instanceof String) {
                    final ByteString b = ByteString.copyFromUtf8((String)ref);
                    return (ByteString)(this.value_ = b);
                }
                return (ByteString)ref;
            }
            
            public Builder setValue(final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x2;
                this.value_ = value;
                this.onChanged();
                return this;
            }
            
            public Builder clearValue() {
                this.bitField0_ &= 0xFFFFFFFD;
                this.value_ = ColumnProperty.getDefaultInstance().getValue();
                this.onChanged();
                return this;
            }
            
            public Builder setValueBytes(final ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x2;
                this.value_ = value;
                this.onChanged();
                return this;
            }
            
            public final Builder setUnknownFields(final UnknownFieldSet unknownFields) {
                return (Builder)super.setUnknownFields(unknownFields);
            }
            
            public final Builder mergeUnknownFields(final UnknownFieldSet unknownFields) {
                return (Builder)super.mergeUnknownFields(unknownFields);
            }
        }
    }
    
    public static final class ColumnProperties extends GeneratedMessageV3 implements ColumnPropertiesOrBuilder
    {
        private static final long serialVersionUID = 0L;
        private int bitField0_;
        public static final int COLUMN_NAME_FIELD_NUMBER = 1;
        private volatile Object columnName_;
        public static final int PROPERTIES_FIELD_NUMBER = 2;
        private List<ColumnProperty> properties_;
        private byte memoizedIsInitialized;
        private static final ColumnProperties DEFAULT_INSTANCE;
        @Deprecated
        public static final Parser<ColumnProperties> PARSER;
        
        private ColumnProperties(final GeneratedMessageV3.Builder<?> builder) {
            super((GeneratedMessageV3.Builder)builder);
            this.memoizedIsInitialized = -1;
        }
        
        private ColumnProperties() {
            this.memoizedIsInitialized = -1;
            this.columnName_ = "";
            this.properties_ = Collections.emptyList();
        }
        
        protected Object newInstance(final GeneratedMessageV3.UnusedPrivateParameter unused) {
            return new ColumnProperties();
        }
        
        public final UnknownFieldSet getUnknownFields() {
            return this.unknownFields;
        }
        
        private ColumnProperties(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            this();
            if (extensionRegistry == null) {
                throw new NullPointerException();
            }
            int mutable_bitField0_ = 0;
            final UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder();
            try {
                boolean done = false;
                while (!done) {
                    final int tag = input.readTag();
                    switch (tag) {
                        case 0: {
                            done = true;
                            continue;
                        }
                        case 10: {
                            final ByteString bs = input.readBytes();
                            this.bitField0_ |= 0x1;
                            this.columnName_ = bs;
                            continue;
                        }
                        case 18: {
                            if ((mutable_bitField0_ & 0x2) == 0x0) {
                                this.properties_ = new ArrayList<ColumnProperty>();
                                mutable_bitField0_ |= 0x2;
                            }
                            this.properties_.add((ColumnProperty)input.readMessage((Parser)ColumnProperty.PARSER, extensionRegistry));
                            continue;
                        }
                        default: {
                            if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                                done = true;
                                continue;
                            }
                            continue;
                        }
                    }
                }
            }
            catch (InvalidProtocolBufferException e) {
                throw e.setUnfinishedMessage((MessageLite)this);
            }
            catch (IOException e2) {
                throw new InvalidProtocolBufferException(e2).setUnfinishedMessage((MessageLite)this);
            }
            finally {
                if ((mutable_bitField0_ & 0x2) != 0x0) {
                    this.properties_ = Collections.unmodifiableList((List<? extends ColumnProperty>)this.properties_);
                }
                this.unknownFields = unknownFields.build();
                this.makeExtensionsImmutable();
            }
        }
        
        public static final Descriptors.Descriptor getDescriptor() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor;
        }
        
        protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_fieldAccessorTable.ensureFieldAccessorsInitialized((Class)ColumnProperties.class, (Class)Builder.class);
        }
        
        public boolean hasColumnName() {
            return (this.bitField0_ & 0x1) != 0x0;
        }
        
        public String getColumnName() {
            final Object ref = this.columnName_;
            if (ref instanceof String) {
                return (String)ref;
            }
            final ByteString bs = (ByteString)ref;
            final String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
                this.columnName_ = s;
            }
            return s;
        }
        
        public ByteString getColumnNameBytes() {
            final Object ref = this.columnName_;
            if (ref instanceof String) {
                final ByteString b = ByteString.copyFromUtf8((String)ref);
                return (ByteString)(this.columnName_ = b);
            }
            return (ByteString)ref;
        }
        
        public List<ColumnProperty> getPropertiesList() {
            return this.properties_;
        }
        
        public List<? extends ColumnPropertyOrBuilder> getPropertiesOrBuilderList() {
            return this.properties_;
        }
        
        public int getPropertiesCount() {
            return this.properties_.size();
        }
        
        public ColumnProperty getProperties(final int index) {
            return this.properties_.get(index);
        }
        
        public ColumnPropertyOrBuilder getPropertiesOrBuilder(final int index) {
            return this.properties_.get(index);
        }
        
        public final boolean isInitialized() {
            final byte isInitialized = this.memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }
            this.memoizedIsInitialized = 1;
            return true;
        }
        
        public void writeTo(final CodedOutputStream output) throws IOException {
            if ((this.bitField0_ & 0x1) != 0x0) {
                GeneratedMessageV3.writeString(output, 1, this.columnName_);
            }
            for (int i = 0; i < this.properties_.size(); ++i) {
                output.writeMessage(2, (MessageLite)this.properties_.get(i));
            }
            this.unknownFields.writeTo(output);
        }
        
        public int getSerializedSize() {
            int size = this.memoizedSize;
            if (size != -1) {
                return size;
            }
            size = 0;
            if ((this.bitField0_ & 0x1) != 0x0) {
                size += GeneratedMessageV3.computeStringSize(1, this.columnName_);
            }
            for (int i = 0; i < this.properties_.size(); ++i) {
                size += CodedOutputStream.computeMessageSize(2, (MessageLite)this.properties_.get(i));
            }
            size += this.unknownFields.getSerializedSize();
            return this.memoizedSize = size;
        }
        
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof ColumnProperties)) {
                return super.equals(obj);
            }
            final ColumnProperties other = (ColumnProperties)obj;
            return this.hasColumnName() == other.hasColumnName() && (!this.hasColumnName() || this.getColumnName().equals(other.getColumnName())) && this.getPropertiesList().equals(other.getPropertiesList()) && this.unknownFields.equals(other.unknownFields);
        }
        
        public int hashCode() {
            if (this.memoizedHashCode != 0) {
                return this.memoizedHashCode;
            }
            int hash = 41;
            hash = 19 * hash + getDescriptor().hashCode();
            if (this.hasColumnName()) {
                hash = 37 * hash + 1;
                hash = 53 * hash + this.getColumnName().hashCode();
            }
            if (this.getPropertiesCount() > 0) {
                hash = 37 * hash + 2;
                hash = 53 * hash + this.getPropertiesList().hashCode();
            }
            hash = 29 * hash + this.unknownFields.hashCode();
            return this.memoizedHashCode = hash;
        }
        
        public static ColumnProperties parseFrom(final ByteBuffer data) throws InvalidProtocolBufferException {
            return (ColumnProperties)ColumnProperties.PARSER.parseFrom(data);
        }
        
        public static ColumnProperties parseFrom(final ByteBuffer data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return (ColumnProperties)ColumnProperties.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static ColumnProperties parseFrom(final ByteString data) throws InvalidProtocolBufferException {
            return (ColumnProperties)ColumnProperties.PARSER.parseFrom(data);
        }
        
        public static ColumnProperties parseFrom(final ByteString data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return (ColumnProperties)ColumnProperties.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static ColumnProperties parseFrom(final byte[] data) throws InvalidProtocolBufferException {
            return (ColumnProperties)ColumnProperties.PARSER.parseFrom(data);
        }
        
        public static ColumnProperties parseFrom(final byte[] data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return (ColumnProperties)ColumnProperties.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static ColumnProperties parseFrom(final InputStream input) throws IOException {
            return (ColumnProperties)GeneratedMessageV3.parseWithIOException((Parser)ColumnProperties.PARSER, input);
        }
        
        public static ColumnProperties parseFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return (ColumnProperties)GeneratedMessageV3.parseWithIOException((Parser)ColumnProperties.PARSER, input, extensionRegistry);
        }
        
        public static ColumnProperties parseDelimitedFrom(final InputStream input) throws IOException {
            return (ColumnProperties)GeneratedMessageV3.parseDelimitedWithIOException((Parser)ColumnProperties.PARSER, input);
        }
        
        public static ColumnProperties parseDelimitedFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return (ColumnProperties)GeneratedMessageV3.parseDelimitedWithIOException((Parser)ColumnProperties.PARSER, input, extensionRegistry);
        }
        
        public static ColumnProperties parseFrom(final CodedInputStream input) throws IOException {
            return (ColumnProperties)GeneratedMessageV3.parseWithIOException((Parser)ColumnProperties.PARSER, input);
        }
        
        public static ColumnProperties parseFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return (ColumnProperties)GeneratedMessageV3.parseWithIOException((Parser)ColumnProperties.PARSER, input, extensionRegistry);
        }
        
        public Builder newBuilderForType() {
            return newBuilder();
        }
        
        public static Builder newBuilder() {
            return ColumnProperties.DEFAULT_INSTANCE.toBuilder();
        }
        
        public static Builder newBuilder(final ColumnProperties prototype) {
            return ColumnProperties.DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }
        
        public Builder toBuilder() {
            return (this == ColumnProperties.DEFAULT_INSTANCE) ? new Builder() : new Builder().mergeFrom(this);
        }
        
        protected Builder newBuilderForType(final GeneratedMessageV3.BuilderParent parent) {
            final Builder builder = new Builder(parent);
            return builder;
        }
        
        public static ColumnProperties getDefaultInstance() {
            return ColumnProperties.DEFAULT_INSTANCE;
        }
        
        public static Parser<ColumnProperties> parser() {
            return ColumnProperties.PARSER;
        }
        
        public Parser<ColumnProperties> getParserForType() {
            return ColumnProperties.PARSER;
        }
        
        public ColumnProperties getDefaultInstanceForType() {
            return ColumnProperties.DEFAULT_INSTANCE;
        }
        
        static {
            DEFAULT_INSTANCE = new ColumnProperties();
            PARSER = (Parser)new AbstractParser<ColumnProperties>() {
                public ColumnProperties parsePartialFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
                    return new ColumnProperties(input, extensionRegistry);
                }
            };
        }
        
        public static final class Builder extends GeneratedMessageV3.Builder<Builder> implements ColumnPropertiesOrBuilder
        {
            private int bitField0_;
            private Object columnName_;
            private List<ColumnProperty> properties_;
            private RepeatedFieldBuilderV3<ColumnProperty, ColumnProperty.Builder, ColumnPropertyOrBuilder> propertiesBuilder_;
            
            public static final Descriptors.Descriptor getDescriptor() {
                return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor;
            }
            
            protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
                return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_fieldAccessorTable.ensureFieldAccessorsInitialized((Class)ColumnProperties.class, (Class)Builder.class);
            }
            
            private Builder() {
                this.columnName_ = "";
                this.properties_ = Collections.emptyList();
                this.maybeForceBuilderInitialization();
            }
            
            private Builder(final GeneratedMessageV3.BuilderParent parent) {
                super(parent);
                this.columnName_ = "";
                this.properties_ = Collections.emptyList();
                this.maybeForceBuilderInitialization();
            }
            
            private void maybeForceBuilderInitialization() {
                if (ColumnProperties.alwaysUseFieldBuilders) {
                    this.getPropertiesFieldBuilder();
                }
            }
            
            public Builder clear() {
                super.clear();
                this.columnName_ = "";
                this.bitField0_ &= 0xFFFFFFFE;
                if (this.propertiesBuilder_ == null) {
                    this.properties_ = Collections.emptyList();
                    this.bitField0_ &= 0xFFFFFFFD;
                }
                else {
                    this.propertiesBuilder_.clear();
                }
                return this;
            }
            
            public Descriptors.Descriptor getDescriptorForType() {
                return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor;
            }
            
            public ColumnProperties getDefaultInstanceForType() {
                return ColumnProperties.getDefaultInstance();
            }
            
            public ColumnProperties build() {
                final ColumnProperties result = this.buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException((Message)result);
                }
                return result;
            }
            
            public ColumnProperties buildPartial() {
                final ColumnProperties result = new ColumnProperties((GeneratedMessageV3.Builder)this);
                final int from_bitField0_ = this.bitField0_;
                int to_bitField0_ = 0;
                if ((from_bitField0_ & 0x1) != 0x0) {
                    to_bitField0_ |= 0x1;
                }
                result.columnName_ = this.columnName_;
                if (this.propertiesBuilder_ == null) {
                    if ((this.bitField0_ & 0x2) != 0x0) {
                        this.properties_ = Collections.unmodifiableList((List<? extends ColumnProperty>)this.properties_);
                        this.bitField0_ &= 0xFFFFFFFD;
                    }
                    result.properties_ = this.properties_;
                }
                else {
                    result.properties_ = (List<ColumnProperty>)this.propertiesBuilder_.build();
                }
                result.bitField0_ = to_bitField0_;
                this.onBuilt();
                return result;
            }
            
            public Builder clone() {
                return (Builder)super.clone();
            }
            
            public Builder setField(final Descriptors.FieldDescriptor field, final Object value) {
                return (Builder)super.setField(field, value);
            }
            
            public Builder clearField(final Descriptors.FieldDescriptor field) {
                return (Builder)super.clearField(field);
            }
            
            public Builder clearOneof(final Descriptors.OneofDescriptor oneof) {
                return (Builder)super.clearOneof(oneof);
            }
            
            public Builder setRepeatedField(final Descriptors.FieldDescriptor field, final int index, final Object value) {
                return (Builder)super.setRepeatedField(field, index, value);
            }
            
            public Builder addRepeatedField(final Descriptors.FieldDescriptor field, final Object value) {
                return (Builder)super.addRepeatedField(field, value);
            }
            
            public Builder mergeFrom(final Message other) {
                if (other instanceof ColumnProperties) {
                    return this.mergeFrom((ColumnProperties)other);
                }
                super.mergeFrom(other);
                return this;
            }
            
            public Builder mergeFrom(final ColumnProperties other) {
                if (other == ColumnProperties.getDefaultInstance()) {
                    return this;
                }
                if (other.hasColumnName()) {
                    this.bitField0_ |= 0x1;
                    this.columnName_ = other.columnName_;
                    this.onChanged();
                }
                if (this.propertiesBuilder_ == null) {
                    if (!other.properties_.isEmpty()) {
                        if (this.properties_.isEmpty()) {
                            this.properties_ = other.properties_;
                            this.bitField0_ &= 0xFFFFFFFD;
                        }
                        else {
                            this.ensurePropertiesIsMutable();
                            this.properties_.addAll(other.properties_);
                        }
                        this.onChanged();
                    }
                }
                else if (!other.properties_.isEmpty()) {
                    if (this.propertiesBuilder_.isEmpty()) {
                        this.propertiesBuilder_.dispose();
                        this.propertiesBuilder_ = null;
                        this.properties_ = other.properties_;
                        this.bitField0_ &= 0xFFFFFFFD;
                        this.propertiesBuilder_ = (ColumnProperties.alwaysUseFieldBuilders ? this.getPropertiesFieldBuilder() : null);
                    }
                    else {
                        this.propertiesBuilder_.addAllMessages((Iterable)other.properties_);
                    }
                }
                this.mergeUnknownFields(other.unknownFields);
                this.onChanged();
                return this;
            }
            
            public final boolean isInitialized() {
                return true;
            }
            
            public Builder mergeFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
                ColumnProperties parsedMessage = null;
                try {
                    parsedMessage = (ColumnProperties)ColumnProperties.PARSER.parsePartialFrom(input, extensionRegistry);
                }
                catch (InvalidProtocolBufferException e) {
                    parsedMessage = (ColumnProperties)e.getUnfinishedMessage();
                    throw e.unwrapIOException();
                }
                finally {
                    if (parsedMessage != null) {
                        this.mergeFrom(parsedMessage);
                    }
                }
                return this;
            }
            
            public boolean hasColumnName() {
                return (this.bitField0_ & 0x1) != 0x0;
            }
            
            public String getColumnName() {
                final Object ref = this.columnName_;
                if (!(ref instanceof String)) {
                    final ByteString bs = (ByteString)ref;
                    final String s = bs.toStringUtf8();
                    if (bs.isValidUtf8()) {
                        this.columnName_ = s;
                    }
                    return s;
                }
                return (String)ref;
            }
            
            public ByteString getColumnNameBytes() {
                final Object ref = this.columnName_;
                if (ref instanceof String) {
                    final ByteString b = ByteString.copyFromUtf8((String)ref);
                    return (ByteString)(this.columnName_ = b);
                }
                return (ByteString)ref;
            }
            
            public Builder setColumnName(final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x1;
                this.columnName_ = value;
                this.onChanged();
                return this;
            }
            
            public Builder clearColumnName() {
                this.bitField0_ &= 0xFFFFFFFE;
                this.columnName_ = ColumnProperties.getDefaultInstance().getColumnName();
                this.onChanged();
                return this;
            }
            
            public Builder setColumnNameBytes(final ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x1;
                this.columnName_ = value;
                this.onChanged();
                return this;
            }
            
            private void ensurePropertiesIsMutable() {
                if ((this.bitField0_ & 0x2) == 0x0) {
                    this.properties_ = new ArrayList<ColumnProperty>(this.properties_);
                    this.bitField0_ |= 0x2;
                }
            }
            
            public List<ColumnProperty> getPropertiesList() {
                if (this.propertiesBuilder_ == null) {
                    return Collections.unmodifiableList((List<? extends ColumnProperty>)this.properties_);
                }
                return (List<ColumnProperty>)this.propertiesBuilder_.getMessageList();
            }
            
            public int getPropertiesCount() {
                if (this.propertiesBuilder_ == null) {
                    return this.properties_.size();
                }
                return this.propertiesBuilder_.getCount();
            }
            
            public ColumnProperty getProperties(final int index) {
                if (this.propertiesBuilder_ == null) {
                    return this.properties_.get(index);
                }
                return (ColumnProperty)this.propertiesBuilder_.getMessage(index);
            }
            
            public Builder setProperties(final int index, final ColumnProperty value) {
                if (this.propertiesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    this.ensurePropertiesIsMutable();
                    this.properties_.set(index, value);
                    this.onChanged();
                }
                else {
                    this.propertiesBuilder_.setMessage(index, value);
                }
                return this;
            }
            
            public Builder setProperties(final int index, final ColumnProperty.Builder builderForValue) {
                if (this.propertiesBuilder_ == null) {
                    this.ensurePropertiesIsMutable();
                    this.properties_.set(index, builderForValue.build());
                    this.onChanged();
                }
                else {
                    this.propertiesBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }
            
            public Builder addProperties(final ColumnProperty value) {
                if (this.propertiesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    this.ensurePropertiesIsMutable();
                    this.properties_.add(value);
                    this.onChanged();
                }
                else {
                    this.propertiesBuilder_.addMessage(value);
                }
                return this;
            }
            
            public Builder addProperties(final int index, final ColumnProperty value) {
                if (this.propertiesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    this.ensurePropertiesIsMutable();
                    this.properties_.add(index, value);
                    this.onChanged();
                }
                else {
                    this.propertiesBuilder_.addMessage(index, value);
                }
                return this;
            }
            
            public Builder addProperties(final ColumnProperty.Builder builderForValue) {
                if (this.propertiesBuilder_ == null) {
                    this.ensurePropertiesIsMutable();
                    this.properties_.add(builderForValue.build());
                    this.onChanged();
                }
                else {
                    this.propertiesBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }
            
            public Builder addProperties(final int index, final ColumnProperty.Builder builderForValue) {
                if (this.propertiesBuilder_ == null) {
                    this.ensurePropertiesIsMutable();
                    this.properties_.add(index, builderForValue.build());
                    this.onChanged();
                }
                else {
                    this.propertiesBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }
            
            public Builder addAllProperties(final Iterable<? extends ColumnProperty> values) {
                if (this.propertiesBuilder_ == null) {
                    this.ensurePropertiesIsMutable();
                    AbstractMessageLite.Builder.addAll((Iterable)values, (List)this.properties_);
                    this.onChanged();
                }
                else {
                    this.propertiesBuilder_.addAllMessages((Iterable)values);
                }
                return this;
            }
            
            public Builder clearProperties() {
                if (this.propertiesBuilder_ == null) {
                    this.properties_ = Collections.emptyList();
                    this.bitField0_ &= 0xFFFFFFFD;
                    this.onChanged();
                }
                else {
                    this.propertiesBuilder_.clear();
                }
                return this;
            }
            
            public Builder removeProperties(final int index) {
                if (this.propertiesBuilder_ == null) {
                    this.ensurePropertiesIsMutable();
                    this.properties_.remove(index);
                    this.onChanged();
                }
                else {
                    this.propertiesBuilder_.remove(index);
                }
                return this;
            }
            
            public ColumnProperty.Builder getPropertiesBuilder(final int index) {
                return (ColumnProperty.Builder)this.getPropertiesFieldBuilder().getBuilder(index);
            }
            
            public ColumnPropertyOrBuilder getPropertiesOrBuilder(final int index) {
                if (this.propertiesBuilder_ == null) {
                    return this.properties_.get(index);
                }
                return (ColumnPropertyOrBuilder)this.propertiesBuilder_.getMessageOrBuilder(index);
            }
            
            public List<? extends ColumnPropertyOrBuilder> getPropertiesOrBuilderList() {
                if (this.propertiesBuilder_ != null) {
                    return (List<? extends ColumnPropertyOrBuilder>)this.propertiesBuilder_.getMessageOrBuilderList();
                }
                return Collections.unmodifiableList((List<? extends ColumnPropertyOrBuilder>)this.properties_);
            }
            
            public ColumnProperty.Builder addPropertiesBuilder() {
                return (ColumnProperty.Builder)this.getPropertiesFieldBuilder().addBuilder(ColumnProperty.getDefaultInstance());
            }
            
            public ColumnProperty.Builder addPropertiesBuilder(final int index) {
                return (ColumnProperty.Builder)this.getPropertiesFieldBuilder().addBuilder(index, ColumnProperty.getDefaultInstance());
            }
            
            public List<ColumnProperty.Builder> getPropertiesBuilderList() {
                return (List<ColumnProperty.Builder>)this.getPropertiesFieldBuilder().getBuilderList();
            }
            
            private RepeatedFieldBuilderV3<ColumnProperty, ColumnProperty.Builder, ColumnPropertyOrBuilder> getPropertiesFieldBuilder() {
                if (this.propertiesBuilder_ == null) {
                    this.propertiesBuilder_ = (RepeatedFieldBuilderV3<ColumnProperty, ColumnProperty.Builder, ColumnPropertyOrBuilder>)new RepeatedFieldBuilderV3((List)this.properties_, (this.bitField0_ & 0x2) != 0x0, (AbstractMessage.BuilderParent)this.getParentForChildren(), this.isClean());
                    this.properties_ = null;
                }
                return this.propertiesBuilder_;
            }
            
            public final Builder setUnknownFields(final UnknownFieldSet unknownFields) {
                return (Builder)super.setUnknownFields(unknownFields);
            }
            
            public final Builder mergeUnknownFields(final UnknownFieldSet unknownFields) {
                return (Builder)super.mergeUnknownFields(unknownFields);
            }
        }
    }
    
    public static final class JdbcTableXattr extends GeneratedMessageV3 implements JdbcTableXattrOrBuilder
    {
        private static final long serialVersionUID = 0L;
        public static final int SKIPPED_COLUMNS_FIELD_NUMBER = 1;
        private LazyStringList skippedColumns_;
        public static final int COLUMN_PROPERTIES_FIELD_NUMBER = 2;
        private List<ColumnProperties> columnProperties_;
        private byte memoizedIsInitialized;
        private static final JdbcTableXattr DEFAULT_INSTANCE;
        @Deprecated
        public static final Parser<JdbcTableXattr> PARSER;
        
        private JdbcTableXattr(final GeneratedMessageV3.Builder<?> builder) {
            super((GeneratedMessageV3.Builder)builder);
            this.memoizedIsInitialized = -1;
        }
        
        private JdbcTableXattr() {
            this.memoizedIsInitialized = -1;
            this.skippedColumns_ = LazyStringArrayList.EMPTY;
            this.columnProperties_ = Collections.emptyList();
        }
        
        protected Object newInstance(final GeneratedMessageV3.UnusedPrivateParameter unused) {
            return new JdbcTableXattr();
        }
        
        public final UnknownFieldSet getUnknownFields() {
            return this.unknownFields;
        }
        
        private JdbcTableXattr(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            this();
            if (extensionRegistry == null) {
                throw new NullPointerException();
            }
            int mutable_bitField0_ = 0;
            final UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder();
            try {
                boolean done = false;
                while (!done) {
                    final int tag = input.readTag();
                    switch (tag) {
                        case 0: {
                            done = true;
                            continue;
                        }
                        case 10: {
                            final ByteString bs = input.readBytes();
                            if ((mutable_bitField0_ & 0x1) == 0x0) {
                                this.skippedColumns_ = (LazyStringList)new LazyStringArrayList();
                                mutable_bitField0_ |= 0x1;
                            }
                            this.skippedColumns_.add(bs);
                            continue;
                        }
                        case 18: {
                            if ((mutable_bitField0_ & 0x2) == 0x0) {
                                this.columnProperties_ = new ArrayList<ColumnProperties>();
                                mutable_bitField0_ |= 0x2;
                            }
                            this.columnProperties_.add((ColumnProperties)input.readMessage((Parser)ColumnProperties.PARSER, extensionRegistry));
                            continue;
                        }
                        default: {
                            if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                                done = true;
                                continue;
                            }
                            continue;
                        }
                    }
                }
            }
            catch (InvalidProtocolBufferException e) {
                throw e.setUnfinishedMessage((MessageLite)this);
            }
            catch (IOException e2) {
                throw new InvalidProtocolBufferException(e2).setUnfinishedMessage((MessageLite)this);
            }
            finally {
                if ((mutable_bitField0_ & 0x1) != 0x0) {
                    this.skippedColumns_ = this.skippedColumns_.getUnmodifiableView();
                }
                if ((mutable_bitField0_ & 0x2) != 0x0) {
                    this.columnProperties_ = Collections.unmodifiableList((List<? extends ColumnProperties>)this.columnProperties_);
                }
                this.unknownFields = unknownFields.build();
                this.makeExtensionsImmutable();
            }
        }
        
        public static final Descriptors.Descriptor getDescriptor() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor;
        }
        
        protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_fieldAccessorTable.ensureFieldAccessorsInitialized((Class)JdbcTableXattr.class, (Class)Builder.class);
        }
        
        public ProtocolStringList getSkippedColumnsList() {
            return (ProtocolStringList)this.skippedColumns_;
        }
        
        public int getSkippedColumnsCount() {
            return this.skippedColumns_.size();
        }
        
        public String getSkippedColumns(final int index) {
            return (String)this.skippedColumns_.get(index);
        }
        
        public ByteString getSkippedColumnsBytes(final int index) {
            return this.skippedColumns_.getByteString(index);
        }
        
        public List<ColumnProperties> getColumnPropertiesList() {
            return this.columnProperties_;
        }
        
        public List<? extends ColumnPropertiesOrBuilder> getColumnPropertiesOrBuilderList() {
            return this.columnProperties_;
        }
        
        public int getColumnPropertiesCount() {
            return this.columnProperties_.size();
        }
        
        public ColumnProperties getColumnProperties(final int index) {
            return this.columnProperties_.get(index);
        }
        
        public ColumnPropertiesOrBuilder getColumnPropertiesOrBuilder(final int index) {
            return this.columnProperties_.get(index);
        }
        
        public final boolean isInitialized() {
            final byte isInitialized = this.memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }
            this.memoizedIsInitialized = 1;
            return true;
        }
        
        public void writeTo(final CodedOutputStream output) throws IOException {
            for (int i = 0; i < this.skippedColumns_.size(); ++i) {
                GeneratedMessageV3.writeString(output, 1, this.skippedColumns_.getRaw(i));
            }
            for (int i = 0; i < this.columnProperties_.size(); ++i) {
                output.writeMessage(2, (MessageLite)this.columnProperties_.get(i));
            }
            this.unknownFields.writeTo(output);
        }
        
        public int getSerializedSize() {
            int size = this.memoizedSize;
            if (size != -1) {
                return size;
            }
            size = 0;
            int dataSize = 0;
            for (int i = 0; i < this.skippedColumns_.size(); ++i) {
                dataSize += computeStringSizeNoTag(this.skippedColumns_.getRaw(i));
            }
            size += dataSize;
            size += 1 * this.getSkippedColumnsList().size();
            for (int j = 0; j < this.columnProperties_.size(); ++j) {
                size += CodedOutputStream.computeMessageSize(2, (MessageLite)this.columnProperties_.get(j));
            }
            size += this.unknownFields.getSerializedSize();
            return this.memoizedSize = size;
        }
        
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof JdbcTableXattr)) {
                return super.equals(obj);
            }
            final JdbcTableXattr other = (JdbcTableXattr)obj;
            return this.getSkippedColumnsList().equals(other.getSkippedColumnsList()) && this.getColumnPropertiesList().equals(other.getColumnPropertiesList()) && this.unknownFields.equals(other.unknownFields);
        }
        
        public int hashCode() {
            if (this.memoizedHashCode != 0) {
                return this.memoizedHashCode;
            }
            int hash = 41;
            hash = 19 * hash + getDescriptor().hashCode();
            if (this.getSkippedColumnsCount() > 0) {
                hash = 37 * hash + 1;
                hash = 53 * hash + this.getSkippedColumnsList().hashCode();
            }
            if (this.getColumnPropertiesCount() > 0) {
                hash = 37 * hash + 2;
                hash = 53 * hash + this.getColumnPropertiesList().hashCode();
            }
            hash = 29 * hash + this.unknownFields.hashCode();
            return this.memoizedHashCode = hash;
        }
        
        public static JdbcTableXattr parseFrom(final ByteBuffer data) throws InvalidProtocolBufferException {
            return (JdbcTableXattr)JdbcTableXattr.PARSER.parseFrom(data);
        }
        
        public static JdbcTableXattr parseFrom(final ByteBuffer data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return (JdbcTableXattr)JdbcTableXattr.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static JdbcTableXattr parseFrom(final ByteString data) throws InvalidProtocolBufferException {
            return (JdbcTableXattr)JdbcTableXattr.PARSER.parseFrom(data);
        }
        
        public static JdbcTableXattr parseFrom(final ByteString data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return (JdbcTableXattr)JdbcTableXattr.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static JdbcTableXattr parseFrom(final byte[] data) throws InvalidProtocolBufferException {
            return (JdbcTableXattr)JdbcTableXattr.PARSER.parseFrom(data);
        }
        
        public static JdbcTableXattr parseFrom(final byte[] data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return (JdbcTableXattr)JdbcTableXattr.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static JdbcTableXattr parseFrom(final InputStream input) throws IOException {
            return (JdbcTableXattr)GeneratedMessageV3.parseWithIOException((Parser)JdbcTableXattr.PARSER, input);
        }
        
        public static JdbcTableXattr parseFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return (JdbcTableXattr)GeneratedMessageV3.parseWithIOException((Parser)JdbcTableXattr.PARSER, input, extensionRegistry);
        }
        
        public static JdbcTableXattr parseDelimitedFrom(final InputStream input) throws IOException {
            return (JdbcTableXattr)GeneratedMessageV3.parseDelimitedWithIOException((Parser)JdbcTableXattr.PARSER, input);
        }
        
        public static JdbcTableXattr parseDelimitedFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return (JdbcTableXattr)GeneratedMessageV3.parseDelimitedWithIOException((Parser)JdbcTableXattr.PARSER, input, extensionRegistry);
        }
        
        public static JdbcTableXattr parseFrom(final CodedInputStream input) throws IOException {
            return (JdbcTableXattr)GeneratedMessageV3.parseWithIOException((Parser)JdbcTableXattr.PARSER, input);
        }
        
        public static JdbcTableXattr parseFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return (JdbcTableXattr)GeneratedMessageV3.parseWithIOException((Parser)JdbcTableXattr.PARSER, input, extensionRegistry);
        }
        
        public Builder newBuilderForType() {
            return newBuilder();
        }
        
        public static Builder newBuilder() {
            return JdbcTableXattr.DEFAULT_INSTANCE.toBuilder();
        }
        
        public static Builder newBuilder(final JdbcTableXattr prototype) {
            return JdbcTableXattr.DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }
        
        public Builder toBuilder() {
            return (this == JdbcTableXattr.DEFAULT_INSTANCE) ? new Builder() : new Builder().mergeFrom(this);
        }
        
        protected Builder newBuilderForType(final GeneratedMessageV3.BuilderParent parent) {
            final Builder builder = new Builder(parent);
            return builder;
        }
        
        public static JdbcTableXattr getDefaultInstance() {
            return JdbcTableXattr.DEFAULT_INSTANCE;
        }
        
        public static Parser<JdbcTableXattr> parser() {
            return JdbcTableXattr.PARSER;
        }
        
        public Parser<JdbcTableXattr> getParserForType() {
            return JdbcTableXattr.PARSER;
        }
        
        public JdbcTableXattr getDefaultInstanceForType() {
            return JdbcTableXattr.DEFAULT_INSTANCE;
        }
        
        static {
            DEFAULT_INSTANCE = new JdbcTableXattr();
            PARSER = (Parser)new AbstractParser<JdbcTableXattr>() {
                public JdbcTableXattr parsePartialFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
                    return new JdbcTableXattr(input, extensionRegistry);
                }
            };
        }
        
        public static final class Builder extends GeneratedMessageV3.Builder<Builder> implements JdbcTableXattrOrBuilder
        {
            private int bitField0_;
            private LazyStringList skippedColumns_;
            private List<ColumnProperties> columnProperties_;
            private RepeatedFieldBuilderV3<ColumnProperties, ColumnProperties.Builder, ColumnPropertiesOrBuilder> columnPropertiesBuilder_;
            
            public static final Descriptors.Descriptor getDescriptor() {
                return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor;
            }
            
            protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
                return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_fieldAccessorTable.ensureFieldAccessorsInitialized((Class)JdbcTableXattr.class, (Class)Builder.class);
            }
            
            private Builder() {
                this.skippedColumns_ = LazyStringArrayList.EMPTY;
                this.columnProperties_ = Collections.emptyList();
                this.maybeForceBuilderInitialization();
            }
            
            private Builder(final GeneratedMessageV3.BuilderParent parent) {
                super(parent);
                this.skippedColumns_ = LazyStringArrayList.EMPTY;
                this.columnProperties_ = Collections.emptyList();
                this.maybeForceBuilderInitialization();
            }
            
            private void maybeForceBuilderInitialization() {
                if (JdbcTableXattr.alwaysUseFieldBuilders) {
                    this.getColumnPropertiesFieldBuilder();
                }
            }
            
            public Builder clear() {
                super.clear();
                this.skippedColumns_ = LazyStringArrayList.EMPTY;
                this.bitField0_ &= 0xFFFFFFFE;
                if (this.columnPropertiesBuilder_ == null) {
                    this.columnProperties_ = Collections.emptyList();
                    this.bitField0_ &= 0xFFFFFFFD;
                }
                else {
                    this.columnPropertiesBuilder_.clear();
                }
                return this;
            }
            
            public Descriptors.Descriptor getDescriptorForType() {
                return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor;
            }
            
            public JdbcTableXattr getDefaultInstanceForType() {
                return JdbcTableXattr.getDefaultInstance();
            }
            
            public JdbcTableXattr build() {
                final JdbcTableXattr result = this.buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException((Message)result);
                }
                return result;
            }
            
            public JdbcTableXattr buildPartial() {
                final JdbcTableXattr result = new JdbcTableXattr((GeneratedMessageV3.Builder)this);
                final int from_bitField0_ = this.bitField0_;
                if ((this.bitField0_ & 0x1) != 0x0) {
                    this.skippedColumns_ = this.skippedColumns_.getUnmodifiableView();
                    this.bitField0_ &= 0xFFFFFFFE;
                }
                result.skippedColumns_ = this.skippedColumns_;
                if (this.columnPropertiesBuilder_ == null) {
                    if ((this.bitField0_ & 0x2) != 0x0) {
                        this.columnProperties_ = Collections.unmodifiableList((List<? extends ColumnProperties>)this.columnProperties_);
                        this.bitField0_ &= 0xFFFFFFFD;
                    }
                    result.columnProperties_ = this.columnProperties_;
                }
                else {
                    result.columnProperties_ = (List<ColumnProperties>)this.columnPropertiesBuilder_.build();
                }
                this.onBuilt();
                return result;
            }
            
            public Builder clone() {
                return (Builder)super.clone();
            }
            
            public Builder setField(final Descriptors.FieldDescriptor field, final Object value) {
                return (Builder)super.setField(field, value);
            }
            
            public Builder clearField(final Descriptors.FieldDescriptor field) {
                return (Builder)super.clearField(field);
            }
            
            public Builder clearOneof(final Descriptors.OneofDescriptor oneof) {
                return (Builder)super.clearOneof(oneof);
            }
            
            public Builder setRepeatedField(final Descriptors.FieldDescriptor field, final int index, final Object value) {
                return (Builder)super.setRepeatedField(field, index, value);
            }
            
            public Builder addRepeatedField(final Descriptors.FieldDescriptor field, final Object value) {
                return (Builder)super.addRepeatedField(field, value);
            }
            
            public Builder mergeFrom(final Message other) {
                if (other instanceof JdbcTableXattr) {
                    return this.mergeFrom((JdbcTableXattr)other);
                }
                super.mergeFrom(other);
                return this;
            }
            
            public Builder mergeFrom(final JdbcTableXattr other) {
                if (other == JdbcTableXattr.getDefaultInstance()) {
                    return this;
                }
                if (!other.skippedColumns_.isEmpty()) {
                    if (this.skippedColumns_.isEmpty()) {
                        this.skippedColumns_ = other.skippedColumns_;
                        this.bitField0_ &= 0xFFFFFFFE;
                    }
                    else {
                        this.ensureSkippedColumnsIsMutable();
                        this.skippedColumns_.addAll((Collection)other.skippedColumns_);
                    }
                    this.onChanged();
                }
                if (this.columnPropertiesBuilder_ == null) {
                    if (!other.columnProperties_.isEmpty()) {
                        if (this.columnProperties_.isEmpty()) {
                            this.columnProperties_ = other.columnProperties_;
                            this.bitField0_ &= 0xFFFFFFFD;
                        }
                        else {
                            this.ensureColumnPropertiesIsMutable();
                            this.columnProperties_.addAll(other.columnProperties_);
                        }
                        this.onChanged();
                    }
                }
                else if (!other.columnProperties_.isEmpty()) {
                    if (this.columnPropertiesBuilder_.isEmpty()) {
                        this.columnPropertiesBuilder_.dispose();
                        this.columnPropertiesBuilder_ = null;
                        this.columnProperties_ = other.columnProperties_;
                        this.bitField0_ &= 0xFFFFFFFD;
                        this.columnPropertiesBuilder_ = (JdbcTableXattr.alwaysUseFieldBuilders ? this.getColumnPropertiesFieldBuilder() : null);
                    }
                    else {
                        this.columnPropertiesBuilder_.addAllMessages((Iterable)other.columnProperties_);
                    }
                }
                this.mergeUnknownFields(other.unknownFields);
                this.onChanged();
                return this;
            }
            
            public final boolean isInitialized() {
                return true;
            }
            
            public Builder mergeFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
                JdbcTableXattr parsedMessage = null;
                try {
                    parsedMessage = (JdbcTableXattr)JdbcTableXattr.PARSER.parsePartialFrom(input, extensionRegistry);
                }
                catch (InvalidProtocolBufferException e) {
                    parsedMessage = (JdbcTableXattr)e.getUnfinishedMessage();
                    throw e.unwrapIOException();
                }
                finally {
                    if (parsedMessage != null) {
                        this.mergeFrom(parsedMessage);
                    }
                }
                return this;
            }
            
            private void ensureSkippedColumnsIsMutable() {
                if ((this.bitField0_ & 0x1) == 0x0) {
                    this.skippedColumns_ = (LazyStringList)new LazyStringArrayList(this.skippedColumns_);
                    this.bitField0_ |= 0x1;
                }
            }
            
            public ProtocolStringList getSkippedColumnsList() {
                return (ProtocolStringList)this.skippedColumns_.getUnmodifiableView();
            }
            
            public int getSkippedColumnsCount() {
                return this.skippedColumns_.size();
            }
            
            public String getSkippedColumns(final int index) {
                return (String)this.skippedColumns_.get(index);
            }
            
            public ByteString getSkippedColumnsBytes(final int index) {
                return this.skippedColumns_.getByteString(index);
            }
            
            public Builder setSkippedColumns(final int index, final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.ensureSkippedColumnsIsMutable();
                this.skippedColumns_.set(index, value);
                this.onChanged();
                return this;
            }
            
            public Builder addSkippedColumns(final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.ensureSkippedColumnsIsMutable();
                this.skippedColumns_.add(value);
                this.onChanged();
                return this;
            }
            
            public Builder addAllSkippedColumns(final Iterable<String> values) {
                this.ensureSkippedColumnsIsMutable();
                AbstractMessageLite.Builder.addAll((Iterable)values, (List)this.skippedColumns_);
                this.onChanged();
                return this;
            }
            
            public Builder clearSkippedColumns() {
                this.skippedColumns_ = LazyStringArrayList.EMPTY;
                this.bitField0_ &= 0xFFFFFFFE;
                this.onChanged();
                return this;
            }
            
            public Builder addSkippedColumnsBytes(final ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.ensureSkippedColumnsIsMutable();
                this.skippedColumns_.add(value);
                this.onChanged();
                return this;
            }
            
            private void ensureColumnPropertiesIsMutable() {
                if ((this.bitField0_ & 0x2) == 0x0) {
                    this.columnProperties_ = new ArrayList<ColumnProperties>(this.columnProperties_);
                    this.bitField0_ |= 0x2;
                }
            }
            
            public List<ColumnProperties> getColumnPropertiesList() {
                if (this.columnPropertiesBuilder_ == null) {
                    return Collections.unmodifiableList((List<? extends ColumnProperties>)this.columnProperties_);
                }
                return (List<ColumnProperties>)this.columnPropertiesBuilder_.getMessageList();
            }
            
            public int getColumnPropertiesCount() {
                if (this.columnPropertiesBuilder_ == null) {
                    return this.columnProperties_.size();
                }
                return this.columnPropertiesBuilder_.getCount();
            }
            
            public ColumnProperties getColumnProperties(final int index) {
                if (this.columnPropertiesBuilder_ == null) {
                    return this.columnProperties_.get(index);
                }
                return (ColumnProperties)this.columnPropertiesBuilder_.getMessage(index);
            }
            
            public Builder setColumnProperties(final int index, final ColumnProperties value) {
                if (this.columnPropertiesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    this.ensureColumnPropertiesIsMutable();
                    this.columnProperties_.set(index, value);
                    this.onChanged();
                }
                else {
                    this.columnPropertiesBuilder_.setMessage(index, value);
                }
                return this;
            }
            
            public Builder setColumnProperties(final int index, final ColumnProperties.Builder builderForValue) {
                if (this.columnPropertiesBuilder_ == null) {
                    this.ensureColumnPropertiesIsMutable();
                    this.columnProperties_.set(index, builderForValue.build());
                    this.onChanged();
                }
                else {
                    this.columnPropertiesBuilder_.setMessage(index, builderForValue.build());
                }
                return this;
            }
            
            public Builder addColumnProperties(final ColumnProperties value) {
                if (this.columnPropertiesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    this.ensureColumnPropertiesIsMutable();
                    this.columnProperties_.add(value);
                    this.onChanged();
                }
                else {
                    this.columnPropertiesBuilder_.addMessage(value);
                }
                return this;
            }
            
            public Builder addColumnProperties(final int index, final ColumnProperties value) {
                if (this.columnPropertiesBuilder_ == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    this.ensureColumnPropertiesIsMutable();
                    this.columnProperties_.add(index, value);
                    this.onChanged();
                }
                else {
                    this.columnPropertiesBuilder_.addMessage(index, value);
                }
                return this;
            }
            
            public Builder addColumnProperties(final ColumnProperties.Builder builderForValue) {
                if (this.columnPropertiesBuilder_ == null) {
                    this.ensureColumnPropertiesIsMutable();
                    this.columnProperties_.add(builderForValue.build());
                    this.onChanged();
                }
                else {
                    this.columnPropertiesBuilder_.addMessage(builderForValue.build());
                }
                return this;
            }
            
            public Builder addColumnProperties(final int index, final ColumnProperties.Builder builderForValue) {
                if (this.columnPropertiesBuilder_ == null) {
                    this.ensureColumnPropertiesIsMutable();
                    this.columnProperties_.add(index, builderForValue.build());
                    this.onChanged();
                }
                else {
                    this.columnPropertiesBuilder_.addMessage(index, builderForValue.build());
                }
                return this;
            }
            
            public Builder addAllColumnProperties(final Iterable<? extends ColumnProperties> values) {
                if (this.columnPropertiesBuilder_ == null) {
                    this.ensureColumnPropertiesIsMutable();
                    AbstractMessageLite.Builder.addAll((Iterable)values, (List)this.columnProperties_);
                    this.onChanged();
                }
                else {
                    this.columnPropertiesBuilder_.addAllMessages((Iterable)values);
                }
                return this;
            }
            
            public Builder clearColumnProperties() {
                if (this.columnPropertiesBuilder_ == null) {
                    this.columnProperties_ = Collections.emptyList();
                    this.bitField0_ &= 0xFFFFFFFD;
                    this.onChanged();
                }
                else {
                    this.columnPropertiesBuilder_.clear();
                }
                return this;
            }
            
            public Builder removeColumnProperties(final int index) {
                if (this.columnPropertiesBuilder_ == null) {
                    this.ensureColumnPropertiesIsMutable();
                    this.columnProperties_.remove(index);
                    this.onChanged();
                }
                else {
                    this.columnPropertiesBuilder_.remove(index);
                }
                return this;
            }
            
            public ColumnProperties.Builder getColumnPropertiesBuilder(final int index) {
                return (ColumnProperties.Builder)this.getColumnPropertiesFieldBuilder().getBuilder(index);
            }
            
            public ColumnPropertiesOrBuilder getColumnPropertiesOrBuilder(final int index) {
                if (this.columnPropertiesBuilder_ == null) {
                    return this.columnProperties_.get(index);
                }
                return (ColumnPropertiesOrBuilder)this.columnPropertiesBuilder_.getMessageOrBuilder(index);
            }
            
            public List<? extends ColumnPropertiesOrBuilder> getColumnPropertiesOrBuilderList() {
                if (this.columnPropertiesBuilder_ != null) {
                    return (List<? extends ColumnPropertiesOrBuilder>)this.columnPropertiesBuilder_.getMessageOrBuilderList();
                }
                return Collections.unmodifiableList((List<? extends ColumnPropertiesOrBuilder>)this.columnProperties_);
            }
            
            public ColumnProperties.Builder addColumnPropertiesBuilder() {
                return (ColumnProperties.Builder)this.getColumnPropertiesFieldBuilder().addBuilder(ColumnProperties.getDefaultInstance());
            }
            
            public ColumnProperties.Builder addColumnPropertiesBuilder(final int index) {
                return (ColumnProperties.Builder)this.getColumnPropertiesFieldBuilder().addBuilder(index, ColumnProperties.getDefaultInstance());
            }
            
            public List<ColumnProperties.Builder> getColumnPropertiesBuilderList() {
                return (List<ColumnProperties.Builder>)this.getColumnPropertiesFieldBuilder().getBuilderList();
            }
            
            private RepeatedFieldBuilderV3<ColumnProperties, ColumnProperties.Builder, ColumnPropertiesOrBuilder> getColumnPropertiesFieldBuilder() {
                if (this.columnPropertiesBuilder_ == null) {
                    this.columnPropertiesBuilder_ = (RepeatedFieldBuilderV3<ColumnProperties, ColumnProperties.Builder, ColumnPropertiesOrBuilder>)new RepeatedFieldBuilderV3((List)this.columnProperties_, (this.bitField0_ & 0x2) != 0x0, (AbstractMessage.BuilderParent)this.getParentForChildren(), this.isClean());
                    this.columnProperties_ = null;
                }
                return this.columnPropertiesBuilder_;
            }
            
            public final Builder setUnknownFields(final UnknownFieldSet unknownFields) {
                return (Builder)super.setUnknownFields(unknownFields);
            }
            
            public final Builder mergeUnknownFields(final UnknownFieldSet unknownFields) {
                return (Builder)super.mergeUnknownFields(unknownFields);
            }
        }
    }
    
    public interface ColumnPropertyOrBuilder extends MessageOrBuilder
    {
        boolean hasKey();
        
        String getKey();
        
        ByteString getKeyBytes();
        
        boolean hasValue();
        
        String getValue();
        
        ByteString getValueBytes();
    }
    
    public interface ColumnPropertiesOrBuilder extends MessageOrBuilder
    {
        boolean hasColumnName();
        
        String getColumnName();
        
        ByteString getColumnNameBytes();
        
        List<ColumnProperty> getPropertiesList();
        
        ColumnProperty getProperties(final int p0);
        
        int getPropertiesCount();
        
        List<? extends ColumnPropertyOrBuilder> getPropertiesOrBuilderList();
        
        ColumnPropertyOrBuilder getPropertiesOrBuilder(final int p0);
    }
    
    public interface JdbcTableXattrOrBuilder extends MessageOrBuilder
    {
        List<String> getSkippedColumnsList();
        
        int getSkippedColumnsCount();
        
        String getSkippedColumns(final int p0);
        
        ByteString getSkippedColumnsBytes(final int p0);
        
        List<ColumnProperties> getColumnPropertiesList();
        
        ColumnProperties getColumnProperties(final int p0);
        
        int getColumnPropertiesCount();
        
        List<? extends ColumnPropertiesOrBuilder> getColumnPropertiesOrBuilderList();
        
        ColumnPropertiesOrBuilder getColumnPropertiesOrBuilder(final int p0);
    }
}
