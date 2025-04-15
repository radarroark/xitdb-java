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

        var header = Database.Header.read(file);
        assertEquals("xit", new String(header.magicNumber));
        assertEquals(0, header.tag);
        assertEquals(0, header.version);
        assertEquals(20, header.hashSize);
        assertEquals(0, header.hashId);
    }
}
