package net.haxy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import net.haxy.Database.Header.InvalidDatabaseException;
import net.haxy.Database.Header.InvalidVersionException;

public class Database {
    File file;
    Header header;

    public Database(File file, Options opts) throws IOException, InvalidDatabaseException, InvalidVersionException {
        this.file = file;

        if (file.length() == 0) {
            this.header = this.writeHeader(opts);
        } else {
            this.header = Header.read(this.file);
            this.header.validate();
        }
    }

    public static record Options (int hashId, short hashSize) {
        public Options() {
            this(0, (short)0);
        }

        public Options(short hashSize) {
            this(0, hashSize);
        }
    }

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

        public static Header read(File file) throws FileNotFoundException, IOException {
            try (var is = new DataInputStream(new FileInputStream(file))) {
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

    private Header writeHeader(Options opts) throws IOException {
        var header = new Header(opts.hashId, opts.hashSize, VERSION, (byte)0, MAGIC_NUMBER);
        try (var os = new DataOutputStream(new FileOutputStream(this.file))) {
            os.write(header.getBytes().array());
        }
        return header;
    }
}