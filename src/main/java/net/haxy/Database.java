package net.haxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Database {
    Core core;
    Header header;

    public Database(Core core, Options opts) throws Exception {
        this.core = core;

        core.seek(0);
        if (core.length() == 0) {
            this.header = this.writeHeader(opts);
        } else {
            this.header = Header.read(core);
            this.header.validate();
        }
    }

    public static record Options (int hashId, short hashSize) {}

    public static final short VERSION = 0;
    public static final byte[] MAGIC_NUMBER = "xit".getBytes();

    public static record Header (
        int hashId,
        short hashSize,
        short version,
        byte tag,
        byte[] magicNumber
    ) {
        public ByteBuffer getBytes() {
            var buffer = ByteBuffer.allocate(12);
            buffer.put(this.magicNumber);
            buffer.put(this.tag);
            buffer.putShort(this.version);
            buffer.putShort(this.hashSize);
            buffer.putInt(this.hashId);
            return buffer;
        }

        public static Header read(Core core) throws IOException {
            var reader = core.getReader();
            var magicNumber = new byte[3];
            reader.readFully(magicNumber);
            var tag = reader.readByte();
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

        public class InvalidDatabaseException extends Exception {}
        public class InvalidVersionException extends Exception {}
    }

    private Header writeHeader(Options opts) throws IOException {
        var header = new Header(opts.hashId, opts.hashSize, VERSION, (byte)0, MAGIC_NUMBER);
        var writer = this.core.getWriter();
        writer.write(header.getBytes().array());
        return header;
    }
}