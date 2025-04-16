package net.haxy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

class DatabaseTest {
    @Test
    void initDatabase() throws Exception {
        var file = File.createTempFile("database", "");
        file.deleteOnExit();

        var core = new CoreFile(file);

        final short hashSize = 20;

        // create db
        new Database(core, new Database.Options(hashSize));

        // read db
        var db = new Database(core, new Database.Options(hashSize));
        assertEquals("xit", new String(db.header.magicNumber()));
        assertEquals(0, db.header.tag());
        assertEquals(0, db.header.version());
        assertEquals(20, db.header.hashSize());
        assertEquals(0, db.header.hashId());
    }
}
