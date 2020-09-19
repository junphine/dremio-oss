package com.dremio.mongo.proto;

import java.nio.*;
import java.io.*;
import java.util.*;
import com.google.protobuf.*;

public final class MongoReaderProto
{
    private static final Descriptors.Descriptor internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor;
    private static final GeneratedMessageV3.FieldAccessorTable internal_static_com_dremio_elastic_proto_MongoTableXattr_fieldAccessorTable;
    private static final Descriptors.Descriptor internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor;
    private static final GeneratedMessageV3.FieldAccessorTable internal_static_com_dremio_elastic_proto_MongoSplitXattr_fieldAccessorTable;
    private static Descriptors.FileDescriptor descriptor;
    
    public static void registerAllExtensions(final ExtensionRegistryLite registry) {
    }
    
    public static void registerAllExtensions(final ExtensionRegistry registry) {
        registerAllExtensions((ExtensionRegistryLite)registry);
    }
    
    public static Descriptors.FileDescriptor getDescriptor() {
        return MongoReaderProto.descriptor;
    }
    
    static {
        final String[] descriptorData = { "\n\u000bmongo.proto\u0012\u0018com.dremio.elastic.proto\"o\n\u000fMongoTableXattr\u0012\u0010\n\bdatabase\u0018\u0001 \u0001(\t\u0012\u0012\n\ncollection\u0018\u0002 \u0001(\t\u00126\n\u0004type\u0018\u0003 \u0001(\u000e2(.com.dremio.elastic.proto.CollectionType\"H\n\u000fMongoSplitXattr\u0012\u0012\n\nmin_filter\u0018\u0001 \u0001(\t\u0012\u0012\n\nmax_filter\u0018\u0002 \u0001(\t\u0012\r\n\u0005hosts\u0018\u0003 \u0003(\t*d\n\u000eCollectionType\u0012\u0014\n\u0010SINGLE_PARTITION\u0010\u0001\u0012\u0013\n\u000fSUB_PARTITIONED\u0010\u0002\u0012\u0012\n\u000eNODE_PARTITION\u0010\u0003\u0012\u0013\n\u000fRANGE_PARTITION\u0010\u0004B,\n\u0016com.dremio.mongo.protoB\u0010MongoReaderProtoH\u0001" };
        MongoReaderProto.descriptor = Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(descriptorData, new Descriptors.FileDescriptor[0]);
        internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor = getDescriptor().getMessageTypes().get(0);
        internal_static_com_dremio_elastic_proto_MongoTableXattr_fieldAccessorTable = new GeneratedMessageV3.FieldAccessorTable(MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor, new String[] { "Database", "Collection", "Type" });
        internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor = getDescriptor().getMessageTypes().get(1);
        internal_static_com_dremio_elastic_proto_MongoSplitXattr_fieldAccessorTable = new GeneratedMessageV3.FieldAccessorTable(MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor, new String[] { "MinFilter", "MaxFilter", "Hosts" });
    }
    
    public enum CollectionType implements ProtocolMessageEnum
    {
        SINGLE_PARTITION(1), 
        SUB_PARTITIONED(2), 
        NODE_PARTITION(3), 
        RANGE_PARTITION(4);
        
        public static final int SINGLE_PARTITION_VALUE = 1;
        public static final int SUB_PARTITIONED_VALUE = 2;
        public static final int NODE_PARTITION_VALUE = 3;
        public static final int RANGE_PARTITION_VALUE = 4;
        private static final Internal.EnumLiteMap<CollectionType> internalValueMap;
        private static final CollectionType[] VALUES;
        private final int value;
        
        public final int getNumber() {
            return this.value;
        }
        
        @Deprecated
        public static CollectionType valueOf(final int value) {
            return forNumber(value);
        }
        
        public static CollectionType forNumber(final int value) {
            switch (value) {
                case 1: {
                    return CollectionType.SINGLE_PARTITION;
                }
                case 2: {
                    return CollectionType.SUB_PARTITIONED;
                }
                case 3: {
                    return CollectionType.NODE_PARTITION;
                }
                case 4: {
                    return CollectionType.RANGE_PARTITION;
                }
                default: {
                    return null;
                }
            }
        }
        
        public static Internal.EnumLiteMap<CollectionType> internalGetValueMap() {
            return CollectionType.internalValueMap;
        }
        
        public final Descriptors.EnumValueDescriptor getValueDescriptor() {
            return getDescriptor().getValues().get(this.getNumber());
        }
        
        public final Descriptors.EnumDescriptor getDescriptorForType() {
            return getDescriptor();
        }
        
        public static final Descriptors.EnumDescriptor getDescriptor() {
            return MongoReaderProto.getDescriptor().getEnumTypes().get(0);
        }
        
        public static CollectionType valueOf(final Descriptors.EnumValueDescriptor desc) {
            if (desc.getType() != getDescriptor()) {
                throw new IllegalArgumentException("EnumValueDescriptor is not for this type.");
            }
            return CollectionType.VALUES[desc.getIndex()];
        }
        
        private CollectionType(final int value) {
            this.value = value;
        }
        
        static {
            internalValueMap = new Internal.EnumLiteMap<CollectionType>() {
                public CollectionType findValueByNumber(final int number) {
                    return CollectionType.forNumber(number);
                }
            };
            VALUES = values();
        }
    }
    
    public static final class MongoTableXattr extends GeneratedMessageV3 implements MongoTableXattrOrBuilder
    {
        private static final long serialVersionUID = 0L;
        private int bitField0_;
        public static final int DATABASE_FIELD_NUMBER = 1;
        private volatile Object database_;
        public static final int COLLECTION_FIELD_NUMBER = 2;
        private volatile Object collection_;
        public static final int TYPE_FIELD_NUMBER = 3;
        private int type_;
        private byte memoizedIsInitialized;
        private static final MongoTableXattr DEFAULT_INSTANCE;
        @Deprecated
        public static final Parser<MongoTableXattr> PARSER;
        
        private MongoTableXattr(final GeneratedMessageV3.Builder<?> builder) {
            super(builder);
            this.memoizedIsInitialized = -1;
        }
        
        private MongoTableXattr() {
            this.memoizedIsInitialized = -1;
            this.database_ = "";
            this.collection_ = "";
            this.type_ = 1;
        }
        
        protected Object newInstance(final GeneratedMessageV3.UnusedPrivateParameter unused) {
            return new MongoTableXattr();
        }
        
        public final UnknownFieldSet getUnknownFields() {
            return this.unknownFields;
        }
        
        private MongoTableXattr(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
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
                            this.database_ = bs;
                            continue;
                        }
                        case 18: {
                            final ByteString bs = input.readBytes();
                            this.bitField0_ |= 0x2;
                            this.collection_ = bs;
                            continue;
                        }
                        case 24: {
                            final int rawValue = input.readEnum();
                            final CollectionType value = CollectionType.valueOf(rawValue);
                            if (value == null) {
                                unknownFields.mergeVarintField(3, rawValue);
                                continue;
                            }
                            this.bitField0_ |= 0x4;
                            this.type_ = rawValue;
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
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor;
        }
        
        protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(MongoTableXattr.class, Builder.class);
        }
        
        public boolean hasDatabase() {
            return (this.bitField0_ & 0x1) != 0x0;
        }
        
        public String getDatabase() {
            final Object ref = this.database_;
            if (ref instanceof String) {
                return (String)ref;
            }
            final ByteString bs = (ByteString)ref;
            final String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
                this.database_ = s;
            }
            return s;
        }
        
        public ByteString getDatabaseBytes() {
            final Object ref = this.database_;
            if (ref instanceof String) {
                final ByteString b = ByteString.copyFromUtf8((String)ref);
                return (ByteString)(this.database_ = b);
            }
            return (ByteString)ref;
        }
        
        public boolean hasCollection() {
            return (this.bitField0_ & 0x2) != 0x0;
        }
        
        public String getCollection() {
            final Object ref = this.collection_;
            if (ref instanceof String) {
                return (String)ref;
            }
            final ByteString bs = (ByteString)ref;
            final String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
                this.collection_ = s;
            }
            return s;
        }
        
        public ByteString getCollectionBytes() {
            final Object ref = this.collection_;
            if (ref instanceof String) {
                final ByteString b = ByteString.copyFromUtf8((String)ref);
                return (ByteString)(this.collection_ = b);
            }
            return (ByteString)ref;
        }
        
        public boolean hasType() {
            return (this.bitField0_ & 0x4) != 0x0;
        }
        
        public CollectionType getType() {
            final CollectionType result = CollectionType.valueOf(this.type_);
            return (result == null) ? CollectionType.SINGLE_PARTITION : result;
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
                GeneratedMessageV3.writeString(output, 1, this.database_);
            }
            if ((this.bitField0_ & 0x2) != 0x0) {
                GeneratedMessageV3.writeString(output, 2, this.collection_);
            }
            if ((this.bitField0_ & 0x4) != 0x0) {
                output.writeEnum(3, this.type_);
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
                size += GeneratedMessageV3.computeStringSize(1, this.database_);
            }
            if ((this.bitField0_ & 0x2) != 0x0) {
                size += GeneratedMessageV3.computeStringSize(2, this.collection_);
            }
            if ((this.bitField0_ & 0x4) != 0x0) {
                size += CodedOutputStream.computeEnumSize(3, this.type_);
            }
            size += this.unknownFields.getSerializedSize();
            return this.memoizedSize = size;
        }
        
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof MongoTableXattr)) {
                return super.equals(obj);
            }
            final MongoTableXattr other = (MongoTableXattr)obj;
            return this.hasDatabase() == other.hasDatabase() && (!this.hasDatabase() || this.getDatabase().equals(other.getDatabase())) && this.hasCollection() == other.hasCollection() && (!this.hasCollection() || this.getCollection().equals(other.getCollection())) && this.hasType() == other.hasType() && (!this.hasType() || this.type_ == other.type_) && this.unknownFields.equals(other.unknownFields);
        }
        
        public int hashCode() {
            if (this.memoizedHashCode != 0) {
                return this.memoizedHashCode;
            }
            int hash = 41;
            hash = 19 * hash + getDescriptor().hashCode();
            if (this.hasDatabase()) {
                hash = 37 * hash + 1;
                hash = 53 * hash + this.getDatabase().hashCode();
            }
            if (this.hasCollection()) {
                hash = 37 * hash + 2;
                hash = 53 * hash + this.getCollection().hashCode();
            }
            if (this.hasType()) {
                hash = 37 * hash + 3;
                hash = 53 * hash + this.type_;
            }
            hash = 29 * hash + this.unknownFields.hashCode();
            return this.memoizedHashCode = hash;
        }
        
        public static MongoTableXattr parseFrom(final ByteBuffer data) throws InvalidProtocolBufferException {
            return MongoTableXattr.PARSER.parseFrom(data);
        }
        
        public static MongoTableXattr parseFrom(final ByteBuffer data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return MongoTableXattr.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static MongoTableXattr parseFrom(final ByteString data) throws InvalidProtocolBufferException {
            return MongoTableXattr.PARSER.parseFrom(data);
        }
        
        public static MongoTableXattr parseFrom(final ByteString data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return MongoTableXattr.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static MongoTableXattr parseFrom(final byte[] data) throws InvalidProtocolBufferException {
            return MongoTableXattr.PARSER.parseFrom(data);
        }
        
        public static MongoTableXattr parseFrom(final byte[] data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return MongoTableXattr.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static MongoTableXattr parseFrom(final InputStream input) throws IOException {
            return GeneratedMessageV3.parseWithIOException(MongoTableXattr.PARSER, input);
        }
        
        public static MongoTableXattr parseFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return GeneratedMessageV3.parseWithIOException(MongoTableXattr.PARSER, input, extensionRegistry);
        }
        
        public static MongoTableXattr parseDelimitedFrom(final InputStream input) throws IOException {
            return GeneratedMessageV3.parseDelimitedWithIOException(MongoTableXattr.PARSER, input);
        }
        
        public static MongoTableXattr parseDelimitedFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return GeneratedMessageV3.parseDelimitedWithIOException(MongoTableXattr.PARSER, input, extensionRegistry);
        }
        
        public static MongoTableXattr parseFrom(final CodedInputStream input) throws IOException {
            return GeneratedMessageV3.parseWithIOException(MongoTableXattr.PARSER, input);
        }
        
        public static MongoTableXattr parseFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return GeneratedMessageV3.parseWithIOException(MongoTableXattr.PARSER, input, extensionRegistry);
        }
        
        public Builder newBuilderForType() {
            return newBuilder();
        }
        
        public static Builder newBuilder() {
            return MongoTableXattr.DEFAULT_INSTANCE.toBuilder();
        }
        
        public static Builder newBuilder(final MongoTableXattr prototype) {
            return MongoTableXattr.DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }
        
        public Builder toBuilder() {
            return (this == MongoTableXattr.DEFAULT_INSTANCE) ? new Builder() : new Builder().mergeFrom(this);
        }
        
        protected Builder newBuilderForType(final GeneratedMessageV3.BuilderParent parent) {
            final Builder builder = new Builder(parent);
            return builder;
        }
        
        public static MongoTableXattr getDefaultInstance() {
            return MongoTableXattr.DEFAULT_INSTANCE;
        }
        
        public static Parser<MongoTableXattr> parser() {
            return MongoTableXattr.PARSER;
        }
        
        public Parser<MongoTableXattr> getParserForType() {
            return MongoTableXattr.PARSER;
        }
        
        public MongoTableXattr getDefaultInstanceForType() {
            return MongoTableXattr.DEFAULT_INSTANCE;
        }
        
        static {
            DEFAULT_INSTANCE = new MongoTableXattr();
            PARSER = new AbstractParser<MongoTableXattr>() {
                public MongoTableXattr parsePartialFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
                    return new MongoTableXattr(input, extensionRegistry);
                }
            };
        }
        
        public static final class Builder extends GeneratedMessageV3.Builder<Builder> implements MongoTableXattrOrBuilder
        {
            private int bitField0_;
            private Object database_;
            private Object collection_;
            private int type_;
            
            public static final Descriptors.Descriptor getDescriptor() {
                return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor;
            }
            
            protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
                return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(MongoTableXattr.class, Builder.class);
            }
            
            private Builder() {
                this.database_ = "";
                this.collection_ = "";
                this.type_ = 1;
                this.maybeForceBuilderInitialization();
            }
            
            private Builder(final GeneratedMessageV3.BuilderParent parent) {
                super(parent);
                this.database_ = "";
                this.collection_ = "";
                this.type_ = 1;
                this.maybeForceBuilderInitialization();
            }
            
            private void maybeForceBuilderInitialization() {
                if (MongoTableXattr.alwaysUseFieldBuilders) {}
            }
            
            public Builder clear() {
                super.clear();
                this.database_ = "";
                this.bitField0_ &= 0xFFFFFFFE;
                this.collection_ = "";
                this.bitField0_ &= 0xFFFFFFFD;
                this.type_ = 1;
                this.bitField0_ &= 0xFFFFFFFB;
                return this;
            }
            
            public Descriptors.Descriptor getDescriptorForType() {
                return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoTableXattr_descriptor;
            }
            
            public MongoTableXattr getDefaultInstanceForType() {
                return MongoTableXattr.getDefaultInstance();
            }
            
            public MongoTableXattr build() {
                final MongoTableXattr result = this.buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }
            
            public MongoTableXattr buildPartial() {
                final MongoTableXattr result = new MongoTableXattr(this);
                final int from_bitField0_ = this.bitField0_;
                int to_bitField0_ = 0;
                if ((from_bitField0_ & 0x1) != 0x0) {
                    to_bitField0_ |= 0x1;
                }
                result.database_ = this.database_;
                if ((from_bitField0_ & 0x2) != 0x0) {
                    to_bitField0_ |= 0x2;
                }
                result.collection_ = this.collection_;
                if ((from_bitField0_ & 0x4) != 0x0) {
                    to_bitField0_ |= 0x4;
                }
                result.type_ = this.type_;
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
                if (other instanceof MongoTableXattr) {
                    return this.mergeFrom((MongoTableXattr)other);
                }
                super.mergeFrom(other);
                return this;
            }
            
            public Builder mergeFrom(final MongoTableXattr other) {
                if (other == MongoTableXattr.getDefaultInstance()) {
                    return this;
                }
                if (other.hasDatabase()) {
                    this.bitField0_ |= 0x1;
                    this.database_ = other.database_;
                    this.onChanged();
                }
                if (other.hasCollection()) {
                    this.bitField0_ |= 0x2;
                    this.collection_ = other.collection_;
                    this.onChanged();
                }
                if (other.hasType()) {
                    this.setType(other.getType());
                }
                this.mergeUnknownFields(other.unknownFields);
                this.onChanged();
                return this;
            }
            
            public final boolean isInitialized() {
                return true;
            }
            
            public Builder mergeFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
                MongoTableXattr parsedMessage = null;
                try {
                    parsedMessage = (MongoTableXattr)MongoTableXattr.PARSER.parsePartialFrom(input, extensionRegistry);
                }
                catch (InvalidProtocolBufferException e) {
                    parsedMessage = (MongoTableXattr)e.getUnfinishedMessage();
                    throw e.unwrapIOException();
                }
                finally {
                    if (parsedMessage != null) {
                        this.mergeFrom(parsedMessage);
                    }
                }
                return this;
            }
            
            public boolean hasDatabase() {
                return (this.bitField0_ & 0x1) != 0x0;
            }
            
            public String getDatabase() {
                final Object ref = this.database_;
                if (!(ref instanceof String)) {
                    final ByteString bs = (ByteString)ref;
                    final String s = bs.toStringUtf8();
                    if (bs.isValidUtf8()) {
                        this.database_ = s;
                    }
                    return s;
                }
                return (String)ref;
            }
            
            public ByteString getDatabaseBytes() {
                final Object ref = this.database_;
                if (ref instanceof String) {
                    final ByteString b = ByteString.copyFromUtf8((String)ref);
                    return (ByteString)(this.database_ = b);
                }
                return (ByteString)ref;
            }
            
            public Builder setDatabase(final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x1;
                this.database_ = value;
                this.onChanged();
                return this;
            }
            
            public Builder clearDatabase() {
                this.bitField0_ &= 0xFFFFFFFE;
                this.database_ = MongoTableXattr.getDefaultInstance().getDatabase();
                this.onChanged();
                return this;
            }
            
            public Builder setDatabaseBytes(final ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x1;
                this.database_ = value;
                this.onChanged();
                return this;
            }
            
            public boolean hasCollection() {
                return (this.bitField0_ & 0x2) != 0x0;
            }
            
            public String getCollection() {
                final Object ref = this.collection_;
                if (!(ref instanceof String)) {
                    final ByteString bs = (ByteString)ref;
                    final String s = bs.toStringUtf8();
                    if (bs.isValidUtf8()) {
                        this.collection_ = s;
                    }
                    return s;
                }
                return (String)ref;
            }
            
            public ByteString getCollectionBytes() {
                final Object ref = this.collection_;
                if (ref instanceof String) {
                    final ByteString b = ByteString.copyFromUtf8((String)ref);
                    return (ByteString)(this.collection_ = b);
                }
                return (ByteString)ref;
            }
            
            public Builder setCollection(final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x2;
                this.collection_ = value;
                this.onChanged();
                return this;
            }
            
            public Builder clearCollection() {
                this.bitField0_ &= 0xFFFFFFFD;
                this.collection_ = MongoTableXattr.getDefaultInstance().getCollection();
                this.onChanged();
                return this;
            }
            
            public Builder setCollectionBytes(final ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x2;
                this.collection_ = value;
                this.onChanged();
                return this;
            }
            
            public boolean hasType() {
                return (this.bitField0_ & 0x4) != 0x0;
            }
            
            public CollectionType getType() {
                final CollectionType result = CollectionType.valueOf(this.type_);
                return (result == null) ? CollectionType.SINGLE_PARTITION : result;
            }
            
            public Builder setType(final CollectionType value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x4;
                this.type_ = value.getNumber();
                this.onChanged();
                return this;
            }
            
            public Builder clearType() {
                this.bitField0_ &= 0xFFFFFFFB;
                this.type_ = 1;
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
    
    public static final class MongoSplitXattr extends GeneratedMessageV3 implements MongoSplitXattrOrBuilder
    {
        private static final long serialVersionUID = 0L;
        private int bitField0_;
        public static final int MIN_FILTER_FIELD_NUMBER = 1;
        private volatile Object minFilter_;
        public static final int MAX_FILTER_FIELD_NUMBER = 2;
        private volatile Object maxFilter_;
        public static final int HOSTS_FIELD_NUMBER = 3;
        private LazyStringList hosts_;
        private byte memoizedIsInitialized;
        private static final MongoSplitXattr DEFAULT_INSTANCE;
        @Deprecated
        public static final Parser<MongoSplitXattr> PARSER;
        
        private MongoSplitXattr(final GeneratedMessageV3.Builder<?> builder) {
            super((GeneratedMessageV3.Builder)builder);
            this.memoizedIsInitialized = -1;
        }
        
        private MongoSplitXattr() {
            this.memoizedIsInitialized = -1;
            this.minFilter_ = "";
            this.maxFilter_ = "";
            this.hosts_ = LazyStringArrayList.EMPTY;
        }
        
        protected Object newInstance(final GeneratedMessageV3.UnusedPrivateParameter unused) {
            return new MongoSplitXattr();
        }
        
        public final UnknownFieldSet getUnknownFields() {
            return this.unknownFields;
        }
        
        private MongoSplitXattr(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
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
                            this.minFilter_ = bs;
                            continue;
                        }
                        case 18: {
                            final ByteString bs = input.readBytes();
                            this.bitField0_ |= 0x2;
                            this.maxFilter_ = bs;
                            continue;
                        }
                        case 26: {
                            final ByteString bs = input.readBytes();
                            if ((mutable_bitField0_ & 0x4) == 0x0) {
                                this.hosts_ = (LazyStringList)new LazyStringArrayList();
                                mutable_bitField0_ |= 0x4;
                            }
                            this.hosts_.add(bs);
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
                if ((mutable_bitField0_ & 0x4) != 0x0) {
                    this.hosts_ = this.hosts_.getUnmodifiableView();
                }
                this.unknownFields = unknownFields.build();
                this.makeExtensionsImmutable();
            }
        }
        
        public static final Descriptors.Descriptor getDescriptor() {
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor;
        }
        
        protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
            return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(MongoSplitXattr.class, Builder.class);
        }
        
        public boolean hasMinFilter() {
            return (this.bitField0_ & 0x1) != 0x0;
        }
        
        public String getMinFilter() {
            final Object ref = this.minFilter_;
            if (ref instanceof String) {
                return (String)ref;
            }
            final ByteString bs = (ByteString)ref;
            final String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
                this.minFilter_ = s;
            }
            return s;
        }
        
        public ByteString getMinFilterBytes() {
            final Object ref = this.minFilter_;
            if (ref instanceof String) {
                final ByteString b = ByteString.copyFromUtf8((String)ref);
                return (ByteString)(this.minFilter_ = b);
            }
            return (ByteString)ref;
        }
        
        public boolean hasMaxFilter() {
            return (this.bitField0_ & 0x2) != 0x0;
        }
        
        public String getMaxFilter() {
            final Object ref = this.maxFilter_;
            if (ref instanceof String) {
                return (String)ref;
            }
            final ByteString bs = (ByteString)ref;
            final String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
                this.maxFilter_ = s;
            }
            return s;
        }
        
        public ByteString getMaxFilterBytes() {
            final Object ref = this.maxFilter_;
            if (ref instanceof String) {
                final ByteString b = ByteString.copyFromUtf8((String)ref);
                return (ByteString)(this.maxFilter_ = b);
            }
            return (ByteString)ref;
        }
        
        public ProtocolStringList getHostsList() {
            return (ProtocolStringList)this.hosts_;
        }
        
        public int getHostsCount() {
            return this.hosts_.size();
        }
        
        public String getHosts(final int index) {
            return (String)this.hosts_.get(index);
        }
        
        public ByteString getHostsBytes(final int index) {
            return this.hosts_.getByteString(index);
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
                GeneratedMessageV3.writeString(output, 1, this.minFilter_);
            }
            if ((this.bitField0_ & 0x2) != 0x0) {
                GeneratedMessageV3.writeString(output, 2, this.maxFilter_);
            }
            for (int i = 0; i < this.hosts_.size(); ++i) {
                GeneratedMessageV3.writeString(output, 3, this.hosts_.getRaw(i));
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
                size += GeneratedMessageV3.computeStringSize(1, this.minFilter_);
            }
            if ((this.bitField0_ & 0x2) != 0x0) {
                size += GeneratedMessageV3.computeStringSize(2, this.maxFilter_);
            }
            int dataSize = 0;
            for (int i = 0; i < this.hosts_.size(); ++i) {
                dataSize += computeStringSizeNoTag(this.hosts_.getRaw(i));
            }
            size += dataSize;
            size += 1 * this.getHostsList().size();
            size += this.unknownFields.getSerializedSize();
            return this.memoizedSize = size;
        }
        
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof MongoSplitXattr)) {
                return super.equals(obj);
            }
            final MongoSplitXattr other = (MongoSplitXattr)obj;
            return this.hasMinFilter() == other.hasMinFilter() && (!this.hasMinFilter() || this.getMinFilter().equals(other.getMinFilter())) && this.hasMaxFilter() == other.hasMaxFilter() && (!this.hasMaxFilter() || this.getMaxFilter().equals(other.getMaxFilter())) && this.getHostsList().equals(other.getHostsList()) && this.unknownFields.equals(other.unknownFields);
        }
        
        public int hashCode() {
            if (this.memoizedHashCode != 0) {
                return this.memoizedHashCode;
            }
            int hash = 41;
            hash = 19 * hash + getDescriptor().hashCode();
            if (this.hasMinFilter()) {
                hash = 37 * hash + 1;
                hash = 53 * hash + this.getMinFilter().hashCode();
            }
            if (this.hasMaxFilter()) {
                hash = 37 * hash + 2;
                hash = 53 * hash + this.getMaxFilter().hashCode();
            }
            if (this.getHostsCount() > 0) {
                hash = 37 * hash + 3;
                hash = 53 * hash + this.getHostsList().hashCode();
            }
            hash = 29 * hash + this.unknownFields.hashCode();
            return this.memoizedHashCode = hash;
        }
        
        public static MongoSplitXattr parseFrom(final ByteBuffer data) throws InvalidProtocolBufferException {
            return MongoSplitXattr.PARSER.parseFrom(data);
        }
        
        public static MongoSplitXattr parseFrom(final ByteBuffer data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return MongoSplitXattr.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static MongoSplitXattr parseFrom(final ByteString data) throws InvalidProtocolBufferException {
            return MongoSplitXattr.PARSER.parseFrom(data);
        }
        
        public static MongoSplitXattr parseFrom(final ByteString data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return MongoSplitXattr.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static MongoSplitXattr parseFrom(final byte[] data) throws InvalidProtocolBufferException {
            return MongoSplitXattr.PARSER.parseFrom(data);
        }
        
        public static MongoSplitXattr parseFrom(final byte[] data, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return MongoSplitXattr.PARSER.parseFrom(data, extensionRegistry);
        }
        
        public static MongoSplitXattr parseFrom(final InputStream input) throws IOException {
            return GeneratedMessageV3.parseWithIOException(MongoSplitXattr.PARSER, input);
        }
        
        public static MongoSplitXattr parseFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return GeneratedMessageV3.parseWithIOException(MongoSplitXattr.PARSER, input, extensionRegistry);
        }
        
        public static MongoSplitXattr parseDelimitedFrom(final InputStream input) throws IOException {
            return GeneratedMessageV3.parseDelimitedWithIOException(MongoSplitXattr.PARSER, input);
        }
        
        public static MongoSplitXattr parseDelimitedFrom(final InputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return GeneratedMessageV3.parseDelimitedWithIOException(MongoSplitXattr.PARSER, input, extensionRegistry);
        }
        
        public static MongoSplitXattr parseFrom(final CodedInputStream input) throws IOException {
            return GeneratedMessageV3.parseWithIOException(MongoSplitXattr.PARSER, input);
        }
        
        public static MongoSplitXattr parseFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws IOException {
            return GeneratedMessageV3.parseWithIOException(MongoSplitXattr.PARSER, input, extensionRegistry);
        }
        
        public Builder newBuilderForType() {
            return newBuilder();
        }
        
        public static Builder newBuilder() {
            return MongoSplitXattr.DEFAULT_INSTANCE.toBuilder();
        }
        
        public static Builder newBuilder(final MongoSplitXattr prototype) {
            return MongoSplitXattr.DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }
        
        public Builder toBuilder() {
            return (this == MongoSplitXattr.DEFAULT_INSTANCE) ? new Builder() : new Builder().mergeFrom(this);
        }
        
        protected Builder newBuilderForType(final GeneratedMessageV3.BuilderParent parent) {
            final Builder builder = new Builder(parent);
            return builder;
        }
        
        public static MongoSplitXattr getDefaultInstance() {
            return MongoSplitXattr.DEFAULT_INSTANCE;
        }
        
        public static Parser<MongoSplitXattr> parser() {
            return MongoSplitXattr.PARSER;
        }
        
        public Parser<MongoSplitXattr> getParserForType() {
            return MongoSplitXattr.PARSER;
        }
        
        public MongoSplitXattr getDefaultInstanceForType() {
            return MongoSplitXattr.DEFAULT_INSTANCE;
        }
        
        static {
            DEFAULT_INSTANCE = new MongoSplitXattr();
            PARSER = new AbstractParser<MongoSplitXattr>() {
                public MongoSplitXattr parsePartialFrom(final CodedInputStream input, final ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
                    return new MongoSplitXattr(input, extensionRegistry);
                }
            };
        }
        
        public static final class Builder extends GeneratedMessageV3.Builder<Builder> implements MongoSplitXattrOrBuilder
        {
            private int bitField0_;
            private Object minFilter_;
            private Object maxFilter_;
            private LazyStringList hosts_;
            
            public static final Descriptors.Descriptor getDescriptor() {
                return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor;
            }
            
            protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
                return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(MongoSplitXattr.class, Builder.class);
            }
            
            private Builder() {
                this.minFilter_ = "";
                this.maxFilter_ = "";
                this.hosts_ = LazyStringArrayList.EMPTY;
                this.maybeForceBuilderInitialization();
            }
            
            private Builder(final GeneratedMessageV3.BuilderParent parent) {
                super(parent);
                this.minFilter_ = "";
                this.maxFilter_ = "";
                this.hosts_ = LazyStringArrayList.EMPTY;
                this.maybeForceBuilderInitialization();
            }
            
            private void maybeForceBuilderInitialization() {
                if (MongoSplitXattr.alwaysUseFieldBuilders) {}
            }
            
            public Builder clear() {
                super.clear();
                this.minFilter_ = "";
                this.bitField0_ &= 0xFFFFFFFE;
                this.maxFilter_ = "";
                this.bitField0_ &= 0xFFFFFFFD;
                this.hosts_ = LazyStringArrayList.EMPTY;
                this.bitField0_ &= 0xFFFFFFFB;
                return this;
            }
            
            public Descriptors.Descriptor getDescriptorForType() {
                return MongoReaderProto.internal_static_com_dremio_elastic_proto_MongoSplitXattr_descriptor;
            }
            
            public MongoSplitXattr getDefaultInstanceForType() {
                return MongoSplitXattr.getDefaultInstance();
            }
            
            public MongoSplitXattr build() {
                final MongoSplitXattr result = this.buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException((Message)result);
                }
                return result;
            }
            
            public MongoSplitXattr buildPartial() {
                final MongoSplitXattr result = new MongoSplitXattr(this);
                final int from_bitField0_ = this.bitField0_;
                int to_bitField0_ = 0;
                if ((from_bitField0_ & 0x1) != 0x0) {
                    to_bitField0_ |= 0x1;
                }
                result.minFilter_ = this.minFilter_;
                if ((from_bitField0_ & 0x2) != 0x0) {
                    to_bitField0_ |= 0x2;
                }
                result.maxFilter_ = this.maxFilter_;
                if ((this.bitField0_ & 0x4) != 0x0) {
                    this.hosts_ = this.hosts_.getUnmodifiableView();
                    this.bitField0_ &= 0xFFFFFFFB;
                }
                result.hosts_ = this.hosts_;
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
                if (other instanceof MongoSplitXattr) {
                    return this.mergeFrom((MongoSplitXattr)other);
                }
                super.mergeFrom(other);
                return this;
            }
            
            public Builder mergeFrom(final MongoSplitXattr other) {
                if (other == MongoSplitXattr.getDefaultInstance()) {
                    return this;
                }
                if (other.hasMinFilter()) {
                    this.bitField0_ |= 0x1;
                    this.minFilter_ = other.minFilter_;
                    this.onChanged();
                }
                if (other.hasMaxFilter()) {
                    this.bitField0_ |= 0x2;
                    this.maxFilter_ = other.maxFilter_;
                    this.onChanged();
                }
                if (!other.hosts_.isEmpty()) {
                    if (this.hosts_.isEmpty()) {
                        this.hosts_ = other.hosts_;
                        this.bitField0_ &= 0xFFFFFFFB;
                    }
                    else {
                        this.ensureHostsIsMutable();
                        this.hosts_.addAll(other.hosts_);
                    }
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
                MongoSplitXattr parsedMessage = null;
                try {
                    parsedMessage = (MongoSplitXattr)MongoSplitXattr.PARSER.parsePartialFrom(input, extensionRegistry);
                }
                catch (InvalidProtocolBufferException e) {
                    parsedMessage = (MongoSplitXattr)e.getUnfinishedMessage();
                    throw e.unwrapIOException();
                }
                finally {
                    if (parsedMessage != null) {
                        this.mergeFrom(parsedMessage);
                    }
                }
                return this;
            }
            
            public boolean hasMinFilter() {
                return (this.bitField0_ & 0x1) != 0x0;
            }
            
            public String getMinFilter() {
                final Object ref = this.minFilter_;
                if (!(ref instanceof String)) {
                    final ByteString bs = (ByteString)ref;
                    final String s = bs.toStringUtf8();
                    if (bs.isValidUtf8()) {
                        this.minFilter_ = s;
                    }
                    return s;
                }
                return (String)ref;
            }
            
            public ByteString getMinFilterBytes() {
                final Object ref = this.minFilter_;
                if (ref instanceof String) {
                    final ByteString b = ByteString.copyFromUtf8((String)ref);
                    return (ByteString)(this.minFilter_ = b);
                }
                return (ByteString)ref;
            }
            
            public Builder setMinFilter(final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x1;
                this.minFilter_ = value;
                this.onChanged();
                return this;
            }
            
            public Builder clearMinFilter() {
                this.bitField0_ &= 0xFFFFFFFE;
                this.minFilter_ = MongoSplitXattr.getDefaultInstance().getMinFilter();
                this.onChanged();
                return this;
            }
            
            public Builder setMinFilterBytes(final ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x1;
                this.minFilter_ = value;
                this.onChanged();
                return this;
            }
            
            public boolean hasMaxFilter() {
                return (this.bitField0_ & 0x2) != 0x0;
            }
            
            public String getMaxFilter() {
                final Object ref = this.maxFilter_;
                if (!(ref instanceof String)) {
                    final ByteString bs = (ByteString)ref;
                    final String s = bs.toStringUtf8();
                    if (bs.isValidUtf8()) {
                        this.maxFilter_ = s;
                    }
                    return s;
                }
                return (String)ref;
            }
            
            public ByteString getMaxFilterBytes() {
                final Object ref = this.maxFilter_;
                if (ref instanceof String) {
                    final ByteString b = ByteString.copyFromUtf8((String)ref);
                    return (ByteString)(this.maxFilter_ = b);
                }
                return (ByteString)ref;
            }
            
            public Builder setMaxFilter(final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x2;
                this.maxFilter_ = value;
                this.onChanged();
                return this;
            }
            
            public Builder clearMaxFilter() {
                this.bitField0_ &= 0xFFFFFFFD;
                this.maxFilter_ = MongoSplitXattr.getDefaultInstance().getMaxFilter();
                this.onChanged();
                return this;
            }
            
            public Builder setMaxFilterBytes(final ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.bitField0_ |= 0x2;
                this.maxFilter_ = value;
                this.onChanged();
                return this;
            }
            
            private void ensureHostsIsMutable() {
                if ((this.bitField0_ & 0x4) == 0x0) {
                    this.hosts_ = (LazyStringList)new LazyStringArrayList(this.hosts_);
                    this.bitField0_ |= 0x4;
                }
            }
            
            public ProtocolStringList getHostsList() {
                return (ProtocolStringList)this.hosts_.getUnmodifiableView();
            }
            
            public int getHostsCount() {
                return this.hosts_.size();
            }
            
            public String getHosts(final int index) {
                return (String)this.hosts_.get(index);
            }
            
            public ByteString getHostsBytes(final int index) {
                return this.hosts_.getByteString(index);
            }
            
            public Builder setHosts(final int index, final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.ensureHostsIsMutable();
                this.hosts_.set(index, value);
                this.onChanged();
                return this;
            }
            
            public Builder addHosts(final String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.ensureHostsIsMutable();
                this.hosts_.add(value);
                this.onChanged();
                return this;
            }
            
            public Builder addAllHosts(final Iterable<String> values) {
                this.ensureHostsIsMutable();
                AbstractMessageLite.Builder.addAll(values, this.hosts_);
                this.onChanged();
                return this;
            }
            
            public Builder clearHosts() {
                this.hosts_ = LazyStringArrayList.EMPTY;
                this.bitField0_ &= 0xFFFFFFFB;
                this.onChanged();
                return this;
            }
            
            public Builder addHostsBytes(final ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                this.ensureHostsIsMutable();
                this.hosts_.add(value);
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
    
    public interface MongoSplitXattrOrBuilder extends MessageOrBuilder
    {
        boolean hasMinFilter();
        
        String getMinFilter();
        
        ByteString getMinFilterBytes();
        
        boolean hasMaxFilter();
        
        String getMaxFilter();
        
        ByteString getMaxFilterBytes();
        
        List<String> getHostsList();
        
        int getHostsCount();
        
        String getHosts(final int p0);
        
        ByteString getHostsBytes(final int p0);
    }
    
    public interface MongoTableXattrOrBuilder extends MessageOrBuilder
    {
        boolean hasDatabase();
        
        String getDatabase();
        
        ByteString getDatabaseBytes();
        
        boolean hasCollection();
        
        String getCollection();
        
        ByteString getCollectionBytes();
        
        boolean hasType();
        
        CollectionType getType();
    }
}
