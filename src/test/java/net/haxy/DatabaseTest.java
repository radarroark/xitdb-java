package net.haxy;

import org.junit.jupiter.api.Test;

import net.haxy.Database.Header.InvalidDatabaseException;
import net.haxy.Database.Header.InvalidVersionException;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;

class DatabaseTest {
    @Test
    void initDatabase() throws IOException, InvalidDatabaseException, InvalidVersionException {
        var file = File.createTempFile("database", "");
        file.deleteOnExit();

        final short hashSize = 20;

        // create db
        new Database(file, new Database.Options(hashSize));

        // read db
        var db = new Database(file, new Database.Options(hashSize));
        assertEquals("xit", new String(db.header.magicNumber));
        assertEquals(0, db.header.tag);
        assertEquals(0, db.header.version);
        assertEquals(20, db.header.hashSize);
        assertEquals(0, db.header.hashId);
    }
}
