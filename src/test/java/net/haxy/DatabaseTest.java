package net.haxy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

class DatabaseTest {
    @Test
    void initDatabase() throws IOException {
        var file = File.createTempFile("database", "");
        file.deleteOnExit();

        var db = new Database(file);

        try (var os = new DataInputStream(new FileInputStream(db.file))) {
            var magicNumber = new byte[3];
            os.read(magicNumber);
            var tag = os.readByte();
            var version = os.readShort();
            var hashSize = os.readShort();
            var hashId = os.readInt();

            assertEquals("xit", new String(magicNumber));
            assertEquals(0, tag);
            assertEquals(0, version);
            assertEquals(20, hashSize);
            assertEquals(0, hashId);
        }
    }
}
