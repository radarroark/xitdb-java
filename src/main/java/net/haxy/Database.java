package net.haxy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Database {
    File file;
    Header header;

    public Database(File file) throws IOException {
        this.file = file;

        if (file.length() == 0) {
            this.header = this.writeHeader();
        } else {
            this.header = Header.read(this.file);
        }
    }

    public static final short VERSION = 0;

    public static class Header {
        int hashId = 0; // TODO: don't hardcode this
        short hashSize = 20; // TODO: don't hardcode this
        short version = VERSION;
        byte tag = 0;
        byte[] magicNumber = "xit".getBytes();
    
        public ByteBuffer getBytes() {
            var buffer = ByteBuffer.allocate(96);
            buffer.put(this.magicNumber);
            buffer.put(this.tag);
            buffer.putShort(this.version);
            buffer.putShort(this.hashSize);
            buffer.putInt(this.hashId);
            return buffer;
        }
    
        public static Header read(File file) throws FileNotFoundException, IOException {
            try (var os = new DataInputStream(new FileInputStream(file))) {
                var header = new Header();
                header.magicNumber = new byte[3];
                os.read(header.magicNumber);
                header.tag = os.readByte();
                header.version = os.readShort();
                header.hashSize = os.readShort();
                header.hashId = os.readInt();
                return header;
            }
        }
    }

    private Header writeHeader() throws IOException {
        var header = new Header();
        try (var os = new DataOutputStream(new FileOutputStream(this.file))) {
            os.write(header.getBytes().array());
        }
        return header;
    }
}