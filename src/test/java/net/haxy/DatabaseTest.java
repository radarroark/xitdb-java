package net.haxy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

class DatabaseTest {
    @Test
    void testLowLevelApi() throws Exception {
        var file = File.createTempFile("database", "");
        file.deleteOnExit();

        var core = new CoreFile(file);
        var opts = new Database.Options(0, (short)20);

        testLowLevelApi(core, opts);
    }

    void testLowLevelApi(Core core, Database.Options opts) throws Exception {
        // create db
        new Database(core, opts);

        // read db
        var db = new Database(core, opts);
        assertEquals("xit", new String(db.header.magicNumber()));
        assertEquals(0, db.header.tag());
        assertEquals(0, db.header.version());
        assertEquals(20, db.header.hashSize());
        assertEquals(0, db.header.hashId());
    }
}