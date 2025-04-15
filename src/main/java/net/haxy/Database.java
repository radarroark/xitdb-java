package net.haxy;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Database {
    File file;
    DatabaseHeader header;

    public Database(File file) throws IOException {
        this.file = file;

        if (file.length() == 0) {
            this.header = new DatabaseHeader();
            this.writeHeader();
        }
    }

    private DatabaseHeader writeHeader() throws IOException {
        var header = new DatabaseHeader();
        try (var os = new DataOutputStream(new FileOutputStream(this.file))) {
            os.write(header.getBytes().array());
        }
        return header;
    }
}

class DatabaseHeader {
    int hashId = 0; // TODO: don't hardcode this
    short hashSize = 20; // TODO: don't hardcode this
    short version = VERSION;
    byte tag = 0;
    String magicNumber = "xit";

    public static final short VERSION = 0;

    public ByteBuffer getBytes() {
        var buffer = ByteBuffer.allocate(96);
        buffer.put(this.magicNumber.getBytes());
        buffer.put(this.tag);
        buffer.putShort(this.version);
        buffer.putShort(this.hashSize);
        buffer.putInt(this.hashId);
        return buffer;
    }
}