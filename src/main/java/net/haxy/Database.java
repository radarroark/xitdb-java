package net.haxy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.haxy.Database.Header.InvalidDatabaseException;
import net.haxy.Database.Header.InvalidVersionException;

public class Database {
    File file;
    Header header;

    public Database(File file) throws IOException, InvalidDatabaseException, InvalidVersionException {
        this.file = file;

        if (file.length() == 0) {
            this.header = this.writeHeader();
        } else {
            this.header = Header.read(this.file);
            this.header.validate();
        }
    }

    public static final short VERSION = 0;
    public static final byte[] MAGIC_NUMBER = "xit".getBytes();

    public static class Header {
        int hashId = 0; // TODO: don't hardcode this
        short hashSize = 20; // TODO: don't hardcode this
        short version = VERSION;
        byte tag = 0;
        byte[] magicNumber = MAGIC_NUMBER;
    
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
                var header = new Header();
                is.read(header.magicNumber);
                header.tag = is.readByte();
                header.version = is.readShort();
                header.hashSize = is.readShort();
                header.hashId = is.readInt();
                return header;
            }
        }

        public void validate() throws InvalidDatabaseException, InvalidVersionException {
            if (!this.magicNumber.equals(MAGIC_NUMBER)) {
                throw new InvalidDatabaseException();
            }
            if (this.version > VERSION) {
                throw new InvalidVersionException();
            }
        }

        public class InvalidDatabaseException extends Exception {}
        public class InvalidVersionException extends Exception {}
    }

    private Header writeHeader() throws IOException {
        var header = new Header();
        try (var os = new DataOutputStream(new FileOutputStream(this.file))) {
            os.write(header.getBytes().array());
        }
        return header;
    }
}