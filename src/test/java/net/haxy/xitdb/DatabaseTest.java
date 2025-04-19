package net.haxy.xitdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.security.MessageDigest;

class DatabaseTest {
    static int MAX_READ_BYTES = 1024;

    @Test
    void testLowLevelApi() throws Exception {
        var file = File.createTempFile("database", "");
        file.deleteOnExit();

        try (var raf = new RandomAccessFile(file, "rw")) {
            var core = new CoreFile(raf);
            var opts = new Database.Options(new Hash(MessageDigest.getInstance("SHA-1"), 0));
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
            assertThrows(Database.InvalidDatabaseException.class, () -> new Database(core, opts));

            // modify the version
            db.core.seek(0);
            writer.writeByte('x');
            db.core.seek(4);
            writer.writeShort(Database.VERSION + 1);

            // re-open with error
            assertThrows(Database.InvalidVersionException.class, () -> new Database(core, opts));
        }

        // save hash id in header
        {
            var hashId = Hash.stringToId("sha1");
            var optsWithHashId = new Database.Options(new Hash(MessageDigest.getInstance("SHA-1"), hashId));

            // make empty database
            core.setLength(0);
            var db = new Database(core, optsWithHashId);

            assertEquals(hashId, db.header.hashId());
            assertEquals("sha1", Hash.idToString(db.header.hashId()));
        }

        // array_list of hash_maps
        {
            core.setLength(0);
            var db = new Database(core, opts);
            var rootCursor = db.rootCursor();

            // write foo -> bar with a writer
            var fooKey = db.hasher.digest("foo".getBytes());
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.Context((cursor) -> {
                    assertEquals(Tag.NONE, cursor.slotPtr.slot().tag());
                    var writer = cursor.getWriter();
                    writer.write("bar".getBytes());
                    writer.finish();
                })
            });

            // read foo
            {
                var barCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                });
                assertEquals(3, barCursor.count());
                var barValue = barCursor.readBytes(MAX_READ_BYTES);
                assertEquals("bar", new String(barValue));

                // TODO: test wrapping ReadCursor.Reader in a BufferedReader
            }
        }
    }
}