package net.haxy.xitdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;

class DatabaseTest {
    @Test
    void testLowLevelApi() throws Exception {
        var file = File.createTempFile("database", "");
        file.deleteOnExit();

        try (var raf = new RandomAccessFile(file, "rw")) {
            var core = new CoreFile(raf);
            var opts = new Database.Options(new Database.HashId(0), (short)20);
            testLowLevelApi(core, opts);
        }
    }

    void testLowLevelApi(Core core, Database.Options opts) throws Exception {
        // open and re-open database
        {
            // make empty database
            core.setLength(0);
            new Database(core, opts);

            // re-open without error
            var db = new Database(core, opts);
            var writer = db.core.getWriter();
            db.core.seek(0);
            writer.writeByte('g');

            // re-open with error
            assertThrows(Database.Header.InvalidDatabaseException.class, () -> new Database(core, opts));

            // modify the version
            db.core.seek(0);
            writer.writeByte('x');
            db.core.seek(4);
            writer.writeShort(Database.VERSION + 1);

            // re-open with error
            assertThrows(Database.Header.InvalidVersionException.class, () -> new Database(core, opts));
        }

        // save hash id in header
        {
            var hashId = Database.HashId.fromString("sha1");
            var optsWithHashId = new Database.Options(hashId, opts.hashSize());

            // make empty database
            core.setLength(0);
            var db = new Database(core, optsWithHashId);

            assertEquals(hashId.id(), db.header.hashId().id());
            assertEquals("sha1", db.header.hashId().toString());
        }
    }
}