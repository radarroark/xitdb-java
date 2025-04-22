package net.haxy.xitdb;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;

public class Database {
    public Core core;
    public MessageDigest md;
    public Header header;
    public Long txStart;

    public static final short VERSION = 0;
    public static final byte[] MAGIC_NUMBER = "xit".getBytes();
    public static final int DATABASE_START = Header.length;
    public static final int BIT_COUNT = 4;
    public static final int SLOT_COUNT = 1 << BIT_COUNT;
    public static final long MASK = SLOT_COUNT - 1;
    public static final BigInteger BIG_MASK = BigInteger.valueOf(MASK);
    public static final int INDEX_BLOCK_SIZE = Slot.length * SLOT_COUNT;

    public static record Header (
        int hashId,
        short hashSize,
        short version,
        Tag tag,
        byte[] magicNumber
    ) {
        public static int length = 12;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.magicNumber);
            buffer.put((byte)this.tag.ordinal());
            buffer.putShort(this.version);
            buffer.putShort(this.hashSize);
            buffer.putInt(this.hashId);
            return buffer.array();
        }

        public static Header read(Core core) throws IOException {
            var reader = core.reader();
            var magicNumber = new byte[3];
            reader.readFully(magicNumber);
            var tag = Tag.valueOf(reader.readByte() & 0b0111_1111);
            var version = reader.readShort();
            var hashSize = reader.readShort();
            var hashId = reader.readInt();
            return new Header(hashId, hashSize, version, tag, magicNumber);
        }

        public void validate() throws InvalidDatabaseException, InvalidVersionException {
            if (!Arrays.equals(this.magicNumber, MAGIC_NUMBER)) {
                throw new InvalidDatabaseException();
            }
            if (this.version > VERSION) {
                throw new InvalidVersionException();
            }
        }

        public Header withTag(Tag tag) {
            return new Header(this.hashId, this.hashSize, this.version, tag, this.magicNumber);
        }
    }

    public static enum WriteMode {
        READ_ONLY,
        READ_WRITE
    }

    public static record ArrayListHeader(long ptr, long size) {
        public static int length = 16;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.putLong(this.size);
            buffer.putLong(this.ptr);
            return buffer.array();
        }

        public static ArrayListHeader fromBytes(byte[] bytes) {
            var buffer = ByteBuffer.wrap(bytes);
            var size = buffer.getLong();
            var ptr = buffer.getLong();
            return new ArrayListHeader(ptr, size);
        }

        public ArrayListHeader withPtr(long ptr) {
            return new ArrayListHeader(ptr, this.size);
        }
    }
    public static record TopLevelArrayListHeader(long fileSize, ArrayListHeader parent) {
        public static int length = 8 + ArrayListHeader.length;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.parent.toBytes());
            buffer.putLong(this.fileSize);
            return buffer.array();
        }
    }
    public static record KeyValuePair(Slot valueSlot, Slot keySlot, byte[] hash) {
        public static int length(int hashSize) {
            return hashSize + Slot.length * 2;
        }

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length(hash.length));
            buffer.put(hash);
            buffer.put(keySlot.toBytes());
            buffer.put(valueSlot.toBytes());
            return buffer.array();
        }

        public static KeyValuePair fromBytes(byte[] bytes, int hashSize) {
            var buffer = ByteBuffer.wrap(bytes);
            var hash = new byte[hashSize];
            buffer.get(hash);
            var keySlotBytes = new byte[Slot.length];
            buffer.get(keySlotBytes);
            var keySlot = Slot.fromBytes(keySlotBytes);
            var valueSlotBytes = new byte[Slot.length];
            buffer.get(valueSlotBytes);
            var valueSlot = Slot.fromBytes(valueSlotBytes);
            return new KeyValuePair(valueSlot, keySlot, hash);
        }
    }

    public static sealed interface PathPart permits ArrayListInit, ArrayListGet, ArrayListAppend, ArrayListSlice, HashMapInit, HashMapGet, HashMapRemove, WriteData, Context {}
    public static record ArrayListInit() implements PathPart {}
    public static record ArrayListGet(long index) implements PathPart {}
    public static record ArrayListAppend() implements PathPart {}
    public static record ArrayListSlice(long size) implements PathPart {}
    public static record HashMapInit() implements PathPart {}
    public static record HashMapGet(HashMapGetTarget target) implements PathPart {}
    public static record HashMapRemove(byte[] hash) implements PathPart {}
    public static record WriteData(WriteableData data) implements PathPart {}
    public static record Context(ContextFunction function) implements PathPart {}

    public static interface ContextFunction {
        public void run(WriteCursor cursor) throws Exception;
    }

    public static sealed interface HashMapGetTarget permits HashMapGetKVPair, HashMapGetKey, HashMapGetValue {}
    public static record HashMapGetKVPair(byte[] hash) implements HashMapGetTarget {}
    public static record HashMapGetKey(byte[] hash) implements HashMapGetTarget {}
    public static record HashMapGetValue(byte[] hash) implements HashMapGetTarget {}

    public static sealed interface WriteableData permits Slot, Uint, Int, Float, Bytes {}
    public static record Uint(long value) implements WriteableData {}
    public static record Int(long value) implements WriteableData {}
    public static record Float(double value) implements WriteableData {}
    public static record Bytes(byte[] value) implements WriteableData {
        public boolean isShort() {
            if (this.value.length > 8) return false;
            for (byte b : this.value) {
                if (b == 0) return false;
            }
            return true;
        }
    }

    public static class DatabaseException extends Exception {}
    public static class NotImplementedException extends DatabaseException {}
    public static class UnreachableException extends DatabaseException {}
    public static class InvalidDatabaseException extends DatabaseException {}
    public static class InvalidVersionException extends DatabaseException {}
    public static class InvalidHashSizeException extends DatabaseException {}
    public static class KeyNotFoundException extends DatabaseException {}
    public static class WriteNotAllowedException extends DatabaseException {}
    public static class UnexpectedTagException extends DatabaseException {}
    public static class CursorNotWriteableException extends DatabaseException {}
    public static class ExpectedTxStartException extends DatabaseException {}
    public static class KeyOffsetExceededException extends DatabaseException {}
    public static class PathPartMustBeAtEndException extends DatabaseException {}
    public static class StreamTooLongException extends DatabaseException {}
    public static class EndOfStreamException extends DatabaseException {}
    public static class InvalidOffsetException extends DatabaseException {}
    public static class ArrayListSliceOutOfBoundsException extends DatabaseException {}

    // init

    public Database(Core core, Hasher hasher) throws IOException, DatabaseException {
        this.core = core;
        this.md = hasher.md();

        core.seek(0);
        if (core.length() == 0) {
            this.header = this.writeHeader(hasher);
        } else {
            this.header = Header.read(core);
            this.header.validate();
            if (this.header.hashSize() != hasher.md().getDigestLength()) {
                throw new InvalidHashSizeException();
            }
            truncate();
        }

        this.txStart = null;
    }

    public WriteCursor rootCursor() {
        return new WriteCursor(new SlotPointer(null, new Slot(DATABASE_START, this.header.tag)), this);
    }

    // private

    private Header writeHeader(Hasher hasher) throws IOException {
        var header = new Header(hasher.id(), (short)hasher.md().getDigestLength(), VERSION, Tag.NONE, MAGIC_NUMBER);
        var writer = this.core.writer();
        writer.write(header.toBytes());
        return header;
    }

    private void truncate() throws IOException, DatabaseException {
        if (this.header.tag() != Tag.ARRAY_LIST) return;

        var rootCursor = rootCursor();
        var listSize = rootCursor.count();

        if (listSize == 0) return;

        this.core.seek(DATABASE_START + ArrayListHeader.length);
        var reader = this.core.reader();
        var headerFileSize = reader.readLong();

        if (headerFileSize == 0) return;

        var fileSize = this.core.length();

        if (fileSize == headerFileSize) return;

        // ignore error because the file may be open in read-only mode
        try {
            this.core.setLength(headerFileSize);
        } catch (IOException e) {}
    }

    private byte[] checkHash(byte[] hash) throws InvalidHashSizeException {
        if (hash.length != this.header.hashSize()) {
            throw new InvalidHashSizeException();
        }
        return hash;
    }

    protected SlotPointer readSlotPointer(WriteMode writeMode, PathPart[] path, SlotPointer slotPtr) throws Exception {
        if (path.length == 0) {
            if (writeMode == WriteMode.READ_ONLY && slotPtr.slot().tag() == Tag.NONE) {
                throw new KeyNotFoundException();
            }
            return slotPtr;
        }
        var part = path[0];

        var isTopLevel = slotPtr.slot().value() == DATABASE_START;

        var isTxStart = isTopLevel && this.header.tag == Tag.ARRAY_LIST && this.txStart == null;
        if (isTxStart) {
            this.txStart = this.core.length();
        }

        try {
            switch (part) {
                case ArrayListInit arrayListInit -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    if (isTopLevel) {
                        var writer = this.core.writer();

                        // if the top level array list hasn't been initialized
                        if (this.header.tag == Tag.NONE) {
                            // write the array list header
                            this.core.seek(DATABASE_START);
                            var arrayListPtr = DATABASE_START + TopLevelArrayListHeader.length;
                            writer.write((new TopLevelArrayListHeader(
                                0,
                                new ArrayListHeader(arrayListPtr, 0))
                            ).toBytes());

                            // write the first block
                            writer.write(new byte[INDEX_BLOCK_SIZE]);

                            // update db header
                            this.core.seek(0);
                            this.header = this.header.withTag(Tag.ARRAY_LIST);
                            writer.write(this.header.toBytes());
                        }

                        var nextSlotPtr = slotPtr.withSlot(slotPtr.slot().withTag(Tag.ARRAY_LIST));
                        return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                    }

                    if (slotPtr.position() == null) throw new CursorNotWriteableException();
                    long position = slotPtr.position();

                    switch (slotPtr.slot().tag()) {
                        case Tag.NONE -> {
                            // if slot was empty, insert the new list
                            var writer = this.core.writer();
                            this.core.seek(this.core.length());
                            var arrayListStart = this.core.length();
                            var arrayListPtr = arrayListStart + ArrayListHeader.length;
                            writer.write(new ArrayListHeader(
                                arrayListPtr,
                                0
                            ).toBytes());
                            writer.write(new byte[INDEX_BLOCK_SIZE]);
                            // make slot point to list
                            var nextSlotPtr = new SlotPointer(position, new Slot(arrayListStart, Tag.ARRAY_LIST));
                            this.core.seek(position);
                            writer.write(nextSlotPtr.slot().toBytes());
                            return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                        }
                        case Tag.ARRAY_LIST -> {
                            var reader = this.core.reader();
                            var writer = this.core.writer();

                            var arrayListStart = slotPtr.slot().value();

                            // copy it to the end unless it was made in this transaction
                            if (this.txStart != null) {
                                if (arrayListStart < this.txStart) {
                                    // read existing block
                                    this.core.seek(arrayListStart);
                                    var headerBytes = new byte[ArrayListHeader.length];
                                    reader.readFully(headerBytes);
                                    var header = ArrayListHeader.fromBytes(headerBytes);
                                    this.core.seek(header.ptr);
                                    var arrayListIndexBlock = new byte[INDEX_BLOCK_SIZE];
                                    reader.readFully(arrayListIndexBlock);
                                    // copy to the end
                                    this.core.seek(this.core.length());
                                    arrayListStart = this.core.length();
                                    var nextArrayListPtr = arrayListStart + ArrayListHeader.length;
                                    header = header.withPtr(nextArrayListPtr);
                                    writer.write(header.toBytes());
                                    writer.write(arrayListIndexBlock);
                                }
                            } else if (this.header.tag() == Tag.ARRAY_LIST) {
                                throw new ExpectedTxStartException();
                            }

                            // make slot point to list
                            var nextSlotPtr = new SlotPointer(position, new Slot(arrayListStart, Tag.ARRAY_LIST));
                            this.core.seek(position);
                            writer.write(nextSlotPtr.slot().toBytes());
                            return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                        }
                        default -> throw new UnexpectedTagException();
                    }
                }
                case ArrayListGet arrayListGet -> {
                    var tag = isTopLevel ? this.header.tag : slotPtr.slot().tag();
                    switch (tag) {
                        case NONE -> throw new KeyNotFoundException();
                        case ARRAY_LIST -> {}
                        default -> throw new UnexpectedTagException();
                    }

                    var nextArrayListStart = slotPtr.slot().value();
                    var index = arrayListGet.index();

                    this.core.seek(nextArrayListStart);
                    var reader = this.core.reader();
                    var headerBytes = new byte[ArrayListHeader.length];
                    reader.readFully(headerBytes);
                    var header = ArrayListHeader.fromBytes(headerBytes);
                    if (index >= header.size || index < -header.size) {
                        throw new KeyNotFoundException();
                    }

                    var key = index < 0 ? header.size - Math.abs(index) : index;
                    var lastKey = header.size - 1;
                    var shift = (byte) (lastKey < SLOT_COUNT ? 0 : Math.log(lastKey) / Math.log(SLOT_COUNT));
                    var finalSlotPtr = readArrayListSlot(header.ptr, key, shift, writeMode, isTopLevel);

                    return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), finalSlotPtr);
                }
                case ArrayListAppend arrayListAppend -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    var tag = isTopLevel ? this.header.tag : slotPtr.slot().tag();
                    if (tag != Tag.ARRAY_LIST) throw new UnexpectedTagException();

                    var nextArrayListStart = slotPtr.slot().value();

                    var appendResult = readArrayListSlotAppend(nextArrayListStart, writeMode, isTopLevel);
                    var finalSlotPtr = readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), appendResult.slotPtr());

                    var writer = this.core.writer();
                    if (isTopLevel) {
                        this.core.seek(this.core.length());
                        var fileSize = this.core.length();
                        var header = new TopLevelArrayListHeader(fileSize, appendResult.header);

                        // update header
                        this.core.seek(nextArrayListStart);
                        writer.write(header.toBytes());
                    } else {
                        // update header
                        this.core.seek(nextArrayListStart);
                        writer.write(appendResult.header().toBytes());
                    }

                    return finalSlotPtr;
                }
                case ArrayListSlice arrayListSlice -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    if (slotPtr.slot().tag() != Tag.ARRAY_LIST) throw new UnexpectedTagException();

                    var nextArrayListStart = slotPtr.slot().value();

                    var sliceHeader = readArrayListSlice(nextArrayListStart, arrayListSlice.size());
                    var finalSlotPtr = readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), slotPtr);

                    // update header
                    var writer = this.core.writer();
                    this.core.seek(nextArrayListStart);
                    writer.write(sliceHeader.toBytes());

                    return finalSlotPtr;
                }
                case HashMapInit hashMapInit -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    if (isTopLevel) {
                        var writer = this.core.writer();

                        // if the top level hash map hasn't been initialized
                        if (this.header.tag == Tag.NONE) {
                            // write the first block
                            this.core.seek(DATABASE_START);
                            writer.write(new byte[INDEX_BLOCK_SIZE]);

                            // update db header
                            this.core.seek(0);
                            this.header = this.header.withTag(Tag.HASH_MAP);
                            writer.write(this.header.toBytes());
                        }

                        var nextSlotPtr = slotPtr.withSlot(slotPtr.slot().withTag(Tag.HASH_MAP));
                        return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                    }

                    if (slotPtr.position() == null) throw new CursorNotWriteableException();
                    long position = slotPtr.position();

                    switch (slotPtr.slot().tag()) {
                        case NONE -> {
                            // if slot was empty, insert the new map
                            var writer = this.core.writer();
                            this.core.seek(this.core.length());
                            var mapStart = this.core.length();
                            writer.write(new byte[INDEX_BLOCK_SIZE]);
                            // make slot point to map
                            var nextSlotPr = new SlotPointer(position, new Slot(mapStart, Tag.HASH_MAP));
                            this.core.seek(position);
                            writer.write(nextSlotPr.slot().toBytes());
                            return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPr);
                        }
                        case HASH_MAP -> {
                            var reader = this.core.reader();
                            var writer = this.core.writer();

                            var mapStart = slotPtr.slot().value();

                            // copy it to the end unless it was made in this transaction
                            if (this.txStart != null) {
                                if (mapStart < this.txStart) {
                                    // read existing block
                                    this.core.seek(mapStart);
                                    var mapIndexBlock = new byte[INDEX_BLOCK_SIZE];
                                    reader.readFully(mapIndexBlock);
                                    // copy to the end
                                    this.core.seek(this.core.length());
                                    mapStart = this.core.length();
                                    writer.write(mapIndexBlock);
                                }
                            } else if (this.header.tag == Tag.ARRAY_LIST) {
                                throw new ExpectedTxStartException();
                            }

                            // make slot point to map
                            var nextSlotPtr = new SlotPointer(position, new Slot(mapStart, Tag.HASH_MAP));
                            this.core.seek(position);
                            writer.write(nextSlotPtr.slot().toBytes());
                            return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                        }
                        default -> throw new UnexpectedTagException();
                    }
                }
                case HashMapGet hashMapGet -> {
                    switch (slotPtr.slot().tag()) {
                        case NONE -> throw new KeyNotFoundException();
                        case HASH_MAP -> {}
                        default -> throw new UnexpectedTagException();
                    }

                    var nextSlotPtr = switch (hashMapGet.target()) {
                        case HashMapGetKVPair kvPairTarget -> readMapSlot(slotPtr.slot().value(), checkHash(kvPairTarget.hash()), (byte)0, writeMode, isTopLevel, hashMapGet.target());
                        case HashMapGetKey keyTarget -> readMapSlot(slotPtr.slot().value(), checkHash(keyTarget.hash()), (byte)0, writeMode, isTopLevel, hashMapGet.target());
                        case HashMapGetValue valueTarget -> readMapSlot(slotPtr.slot().value(), checkHash(valueTarget.hash()), (byte)0, writeMode, isTopLevel, hashMapGet.target());
                    };

                    return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                }
                case HashMapRemove hashMapRemove -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    switch (slotPtr.slot().tag()) {
                        case NONE -> throw new KeyNotFoundException();
                        case HASH_MAP -> {}
                        default -> throw new UnexpectedTagException();
                    }

                    removeMapSlot(slotPtr.slot().value(), checkHash(hashMapRemove.hash()), (byte)0, isTopLevel);

                    return slotPtr;
                }
                case WriteData writeData -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    if (slotPtr.position() == null) throw new CursorNotWriteableException();
                    long position = slotPtr.position();

                    var writer = this.core.writer();

                    var data = writeData.data();
                    var slot = data == null ? new Slot() : switch (data) {
                        case Slot s -> s;
                        case Uint i -> {
                            if (i.value() < 0) {
                                throw new IllegalArgumentException("Uint must not be negative");
                            }
                            yield new Slot(i.value(), Tag.UINT);
                        }
                        case Int i -> new Slot(i.value(), Tag.INT);
                        case Float f -> {
                            var buffer = ByteBuffer.allocate(8);
                            buffer.putDouble(f.value());
                            buffer.position(0);
                            yield new Slot(buffer.getLong(), Tag.FLOAT);
                        }
                        case Bytes bytes -> {
                            if (bytes.isShort()) {
                                var buffer = ByteBuffer.allocate(8);
                                buffer.put(bytes.value());
                                buffer.position(0);
                                yield new Slot(buffer.getLong(), Tag.SHORT_BYTES);
                            } else {
                                var nextCursor = new WriteCursor(slotPtr, this);
                                var cursorWriter = nextCursor.writer();
                                cursorWriter.write(bytes.value());
                                cursorWriter.finish();
                                yield cursorWriter.slot;
                            }
                        }
                    };

                    // this bit allows us to distinguish between a slot explicitly set to NONE
                    // and a slot that hasn't been set yet
                    if (slot.tag() == Tag.NONE) {
                        slot = slot.withFull(true);
                    }

                    this.core.seek(position);
                    writer.write(slot.toBytes());

                    var nextSlotPtr = new SlotPointer(slotPtr.position(), slot);
                    return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                }
                case Context context -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    if (path.length > 1) throw new PathPartMustBeAtEndException();

                    var nextCursor = new WriteCursor(slotPtr, this);
                    try {
                        context.function().run(nextCursor);
                    } catch (Exception e) {
                        // since an error occured, there may be inaccessible
                        // junk at the end of the db, so delete it if possible
                        try {
                            truncate();
                        } catch (Exception e2) {}
                        throw e;
                    }
                    return nextCursor.slotPtr;
                }
            }
        } finally {
            if (isTxStart) {
                this.txStart = null;
            }
        }
    }

    // hash_map

    private SlotPointer readMapSlot(long indexPos, byte[] keyHash, byte keyOffset, WriteMode writeMode, boolean isTopLevel, HashMapGetTarget target) throws IOException, DatabaseException {
        if (keyOffset > (this.header.hashSize() * 8) / BIT_COUNT) {
            throw new KeyOffsetExceededException();
        }

        var reader = this.core.reader();
        var writer = this.core.writer();

        var i = new BigInteger(keyHash).shiftRight(keyOffset * BIT_COUNT).and(BIG_MASK).intValueExact();
        var slotPos = indexPos + (Slot.length * i);
        this.core.seek(slotPos);
        var slotBytes = new byte[Slot.length];
        reader.readFully(slotBytes);
        var slot = Slot.fromBytes(slotBytes);

        var ptr = slot.value();

        switch (slot.tag()) {
            case NONE -> {
                switch (writeMode) {
                    case READ_ONLY -> throw new KeyNotFoundException();
                    case READ_WRITE -> {
                        this.core.seek(this.core.length());

                        // write hash and key/val slots
                        var hashPos = this.core.length();
                        var keySlotPos = hashPos + this.header.hashSize();
                        var valueSlotPos = keySlotPos + Slot.length;
                        var kvPair = new KeyValuePair(new Slot(), new Slot(), keyHash);
                        writer.write(kvPair.toBytes());

                        // point slot to hash pos
                        var nextSlot = new Slot(hashPos, Tag.KV_PAIR);
                        this.core.seek(slotPos);
                        writer.write(nextSlot.toBytes());

                        return switch (target) {
                            case HashMapGetKVPair kvPairTarget -> new SlotPointer(slotPos, nextSlot);
                            case HashMapGetKey keyTarget -> new SlotPointer(keySlotPos, kvPair.keySlot());
                            case HashMapGetValue valueTarget -> new SlotPointer(valueSlotPos, kvPair.valueSlot());
                        };
                    }
                    default -> throw new UnreachableException();
                }
            }
            case INDEX -> {
                var nextPtr = ptr;
                if (writeMode == WriteMode.READ_WRITE && !isTopLevel) {
                    if (this.txStart != null) {
                        if (nextPtr < this.txStart) {
                            // read existing block
                            this.core.seek(ptr);
                            var indexBlock = new byte[INDEX_BLOCK_SIZE];
                            reader.readFully(indexBlock);
                            // copy it to the end
                            this.core.seek(this.core.length());
                            nextPtr = this.core.length();
                            writer.write(indexBlock);
                            // make slot point to block
                            this.core.seek(slotPos);
                            writer.write(new Slot(nextPtr, Tag.INDEX).toBytes());
                        }
                    } else if (this.header.tag() == Tag.ARRAY_LIST) {
                        throw new ExpectedTxStartException();
                    }
                }
                return readMapSlot(nextPtr, keyHash, (byte) (keyOffset + 1), writeMode, isTopLevel, target);
            }
            case KV_PAIR -> {
                this.core.seek(ptr);
                var kvPairBytes = new byte[KeyValuePair.length(this.header.hashSize())];
                reader.readFully(kvPairBytes);
                var kvPair = KeyValuePair.fromBytes(kvPairBytes, this.header.hashSize());

                if (Arrays.equals(kvPair.hash(), keyHash)) {
                    if (writeMode == WriteMode.READ_WRITE && !isTopLevel) {
                        if (this.txStart != null) {
                            if (ptr < this.txStart) {
                                this.core.seek(this.core.length());

                                // write hash and key/val slots
                                var hashPos = this.core.length();
                                var keySlotPos = hashPos + this.header.hashSize();
                                var valueSlotPos = keySlotPos + Slot.length;
                                writer.write(kvPair.toBytes());

                                // point slot to hash pos
                                var nextSlot = new Slot(hashPos, Tag.KV_PAIR);
                                this.core.seek(slotPos);
                                writer.write(nextSlot.toBytes());

                                return switch (target) {
                                    case HashMapGetKVPair kvPairTarget -> new SlotPointer(slotPos, nextSlot);
                                    case HashMapGetKey keyTarget -> new SlotPointer(keySlotPos, kvPair.keySlot());
                                    case HashMapGetValue valueTarget -> new SlotPointer(valueSlotPos, kvPair.valueSlot());
                                };
                            }
                        } else if (this.header.tag() == Tag.ARRAY_LIST) {
                            throw new ExpectedTxStartException();
                        }
                    }

                    var keySlotPos = ptr + this.header.hashSize();
                    var valueSlotPos = keySlotPos + Slot.length;
                    return switch (target) {
                        case HashMapGetKVPair kvPairTarget -> new SlotPointer(slotPos, slot);
                        case HashMapGetKey keyTarget -> new SlotPointer(keySlotPos, kvPair.keySlot());
                        case HashMapGetValue valueTarget -> new SlotPointer(valueSlotPos, kvPair.valueSlot());
                    };
                } else {
                    switch (writeMode) {
                        case READ_ONLY -> throw new KeyNotFoundException();
                        case READ_WRITE -> {
                            // append new index block
                            if (keyOffset + 1 >= (this.header.hashSize() * 8) / BIT_COUNT) {
                                throw new KeyOffsetExceededException();
                            }
                            var nextI = new BigInteger(kvPair.hash()).shiftRight((keyOffset + 1) * BIT_COUNT).and(BIG_MASK).intValueExact();
                            this.core.seek(this.core.length());
                            var nextIndexPos = this.core.length();
                            writer.write(new byte[INDEX_BLOCK_SIZE]);
                            this.core.seek(nextIndexPos + (Slot.length * nextI));
                            writer.write(slot.toBytes());
                            var nextSlotPtr = readMapSlot(nextIndexPos, keyHash, (byte) (keyOffset + 1), writeMode, isTopLevel, target);
                            this.core.seek(slotPos);
                            writer.write(new Slot(nextIndexPos, Tag.INDEX).toBytes());
                            return nextSlotPtr;
                        }
                        default -> throw new UnreachableException();
                    }
                }
            }
            default -> throw new UnexpectedTagException();
        }
    }

    private Slot removeMapSlot(long indexPos, byte[] keyHash, byte keyOffset, boolean isTopLevel) throws IOException, DatabaseException {
        if (keyOffset > (this.header.hashSize() * 8) / BIT_COUNT) {
            throw new KeyOffsetExceededException();
        }

        var reader = this.core.reader();
        var writer = this.core.writer();

        // read block
        var slotBlock = new Slot[SLOT_COUNT];
        this.core.seek(indexPos);
        var indexBlock = new byte[INDEX_BLOCK_SIZE];
        reader.readFully(indexBlock);
        var buffer = ByteBuffer.wrap(indexBlock);
        for (int i = 0; i < slotBlock.length; i++) {
            var slotBytes = new byte[Slot.length];
            buffer.get(slotBytes);
            slotBlock[i] = Slot.fromBytes(slotBytes);
        }

        // get the current slot
        var i = new BigInteger(keyHash).shiftRight(keyOffset * BIT_COUNT).and(BIG_MASK).intValueExact();
        var slotPos = indexPos + (Slot.length * i);
        var slot = slotBlock[i];

        // get the slot that will replace the current slot
        var nextSlot = switch (slot.tag()) {
            case NONE -> throw new KeyNotFoundException();
            case INDEX -> removeMapSlot(slot.value(), keyHash, (byte) (keyOffset + 1), isTopLevel);
            case KV_PAIR -> {
                this.core.seek(slot.value());
                var kvPairBytes = new byte[KeyValuePair.length(this.header.hashSize())];
                reader.readFully(kvPairBytes);
                var kvPair = KeyValuePair.fromBytes(kvPairBytes, this.header.hashSize());
                if (Arrays.equals(kvPair.hash(), keyHash)) {
                    yield new Slot();
                } else {
                    throw new KeyNotFoundException();
                }
            }
            default -> throw new UnexpectedTagException();
        };

        // if we're the root node, just write the new slot and finish
        if (keyOffset == 0) {
            this.core.seek(slotPos);
            writer.write(nextSlot.toBytes());
            return new Slot(indexPos, Tag.INDEX);
        }

        // get slot to return if there is only one used slot
        // in this index block
        var slotToReturnMaybe = new Slot();
        slotBlock[i] = nextSlot;
        for (Slot blockSlot : slotBlock) {
            if (blockSlot.tag() == Tag.NONE) continue;

            // if there is already a slot to return, that
            // means there is more than one used slot in this
            // index block, so we can't return just a single slot
            if (slotToReturnMaybe != null) {
                if (slotToReturnMaybe.tag() != Tag.NONE) {
                    slotToReturnMaybe = null;
                    break;
                }
            }

            slotToReturnMaybe = blockSlot;
        }

        // if there were either no used slots, or a single KV_PAIR
        // slot, this index block doesn't need to exist anymore
        if (slotToReturnMaybe != null) {
            switch (slotToReturnMaybe.tag()) {
                case Tag.NONE, Tag.KV_PAIR -> {
                    return slotToReturnMaybe;
                }
                default -> {}
            }
        }

        // there was more than one used slot, or a single INDEX slot,
        // so we must keep this index block

        if (!isTopLevel) {
            if (this.txStart != null) {
                if (indexPos < this.txStart) {
                    // copy index block to the end
                    this.core.seek(this.core.length());
                    var nextIndexPos = this.core.length();
                    writer.write(indexBlock);
                    // update the slot
                    var nextSlotPos = nextIndexPos + (Slot.length * i);
                    this.core.seek(nextSlotPos);
                    writer.write(nextSlot.toBytes());
                    return new Slot(nextIndexPos, Tag.INDEX);
                }
            } else if (this.header.tag() == Tag.ARRAY_LIST) {
                throw new ExpectedTxStartException();
            }
        }

        this.core.seek(slotPos);
        writer.write(nextSlot.toBytes());
        return new Slot(indexPos, Tag.INDEX);
    }

    // array_list

    public static record ArrayListAppendResult(ArrayListHeader header, SlotPointer slotPtr) {}

    private ArrayListAppendResult readArrayListSlotAppend(long indexStart, WriteMode writeMode, boolean isTopLevel) throws IOException, DatabaseException {
        var reader = this.core.reader();
        var writer = this.core.writer();

        this.core.seek(indexStart);
        var headerBytes = new byte[ArrayListHeader.length];
        reader.readFully(headerBytes);
        var header = ArrayListHeader.fromBytes(headerBytes);
        var indexPos = header.ptr();

        var key = header.size;

        var prevShift = (byte) (key < SLOT_COUNT ? 0 : Math.log(key - 1) / Math.log(SLOT_COUNT));
        var nextShift = (byte) (key < SLOT_COUNT ? 0 : Math.log(key) / Math.log(SLOT_COUNT));

        if (prevShift != nextShift) {
            // root overflow
            this.core.seek(this.core.length());
            var nextIndexPos = this.core.length();
            writer.write(new byte[INDEX_BLOCK_SIZE]);
            this.core.seek(nextIndexPos);
            writer.write(new Slot(indexPos, Tag.INDEX).toBytes());
            indexPos = nextIndexPos;
        }

        var slotPtr = readArrayListSlot(indexPos, key, nextShift, writeMode, isTopLevel);
        return new ArrayListAppendResult(new ArrayListHeader(indexPos, header.size() + 1), slotPtr);
    }

    private SlotPointer readArrayListSlot(long indexPos, long key, byte shift, WriteMode writeMode, boolean isTopLevel) throws IOException, DatabaseException {
        var reader = this.core.reader();

        var i = (key >> (shift * BIT_COUNT)) & MASK;
        var slotPos = indexPos + (Slot.length * i);
        this.core.seek(slotPos);
        var slotBytes = new byte[Slot.length];
        reader.readFully(slotBytes);
        var slot = Slot.fromBytes(slotBytes);

        if (shift == 0) {
            return new SlotPointer(slotPos, slot);
        }

        var ptr = slot.value();

        switch (slot.tag()) {
            case NONE -> {
                switch (writeMode) {
                    case READ_ONLY -> throw new KeyNotFoundException();
                    case READ_WRITE -> {
                        var writer = this.core.writer();
                        this.core.seek(this.core.length());
                        var nextIndexPos = this.core.length();
                        writer.write(new byte[INDEX_BLOCK_SIZE]);
                        // if top level array list, update the file size in the list
                        // header to prevent truncation from destroying this block
                        if (isTopLevel) {
                            this.core.seek(this.core.length());
                            var fileSize = this.core.length();
                            this.core.seek(DATABASE_START + ArrayListHeader.length);
                            writer.writeLong(fileSize);
                        }
                        this.core.seek(slotPos);
                        writer.write(new Slot(nextIndexPos, Tag.INDEX).toBytes());
                        return readArrayListSlot(nextIndexPos, key, (byte)(shift - 1), writeMode, isTopLevel);
                    }
                    default -> throw new UnreachableException();
                }
            }
            case INDEX -> {
                var nextPtr = ptr;
                if (writeMode == WriteMode.READ_WRITE && !isTopLevel) {
                    if (this.txStart != null) {
                        if (nextPtr < this.txStart) {
                            // read existing block
                            this.core.seek(ptr);
                            var indexBlock = new byte[INDEX_BLOCK_SIZE];
                            reader.readFully(indexBlock);
                            // copy it to the end
                            var writer = this.core.writer();
                            this.core.seek(this.core.length());
                            nextPtr = this.core.length();
                            writer.write(indexBlock);
                            // make slot point to block
                            this.core.seek(slotPos);
                            writer.write(new Slot(nextPtr, Tag.INDEX).toBytes());
                        }
                    } else if (this.header.tag() == Tag.ARRAY_LIST) {
                        throw new ExpectedTxStartException();
                    }
                }
                return readArrayListSlot(nextPtr, key, (byte)(shift - 1), writeMode, isTopLevel);
            }
            default -> throw new UnexpectedTagException();
        }
    }

    private ArrayListHeader readArrayListSlice(long indexStart, long size) throws IOException, DatabaseException {
        var reader = this.core.reader();

        this.core.seek(indexStart);
        var headerBytes = new byte[ArrayListHeader.length];
        reader.readFully(headerBytes);
        var header = ArrayListHeader.fromBytes(headerBytes);

        if (size > header.size() || size < 0) {
            throw new ArrayListSliceOutOfBoundsException();
        }

        var prevShift = (byte) (header.size < SLOT_COUNT + 1 ? 0 : Math.log(header.size - 1) / Math.log(SLOT_COUNT));
        var nextShift = (byte) (size < SLOT_COUNT + 1 ? 0 : Math.log(size - 1) / Math.log(SLOT_COUNT));

        if (prevShift == nextShift) {
            // the root node doesn't need to change
            return new ArrayListHeader(header.ptr, size);
        } else {
            // keep following the first slot until we are at the correct shift
            var shift = prevShift;
            var indexPos = header.ptr;
            while (shift > nextShift) {
                this.core.seek(indexPos);
                var slotBytes = new byte[Slot.length];
                reader.readFully(slotBytes);
                var slot = Slot.fromBytes(slotBytes);
                shift -= 1;
                indexPos = slot.value();
            }
            return new ArrayListHeader(indexPos, size);
        }
    }
}