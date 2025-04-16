package net.haxy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Database {
    Core core;
    Header header;

    public Database(Core core, Options opts) throws Exception {
        this.core = core;

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

        public static Header read(Core core) throws Exception {
            try (var is = new DataInputStream(core.getInputStream())) {
                var magicNumber = new byte[3];
                is.read(magicNumber);
                var tag = is.readByte();
                var version = is.readShort();
                var hashSize = is.readShort();
                var hashId = is.readInt();
                return new Header(hashId, hashSize, version, tag, magicNumber);
            }
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

    private Header writeHeader(Options opts) throws Exception {
        var header = new Header(opts.hashId, opts.hashSize, VERSION, (byte)0, MAGIC_NUMBER);
        try (var os = new DataOutputStream(this.core.getOutputStream())) {
            os.write(header.getBytes().array());
        }
        return header;
    }
}