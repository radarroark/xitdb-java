package net.haxy;

import org.junit.jupiter.api.Test;

import net.haxy.Database.Header.InvalidDatabaseException;
import net.haxy.Database.Header.InvalidVersionException;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

class DatabaseTest {
    @Test
    void initDatabase() throws IOException, InvalidDatabaseException, InvalidVersionException {
        var file = File.createTempFile("database", "");
        file.deleteOnExit();

        // create db
        new Database(file);

        // read db
        var db = new Database(file);
        assertEquals("xit", new String(db.header.magicNumber));
        assertEquals(0, db.header.tag);
        assertEquals(0, db.header.version);
        assertEquals(20, db.header.hashSize);
        assertEquals(0, db.header.hashId);
    }
}
