package net.haxy.xitdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.security.MessageDigest;

class DatabaseTest {
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

        {
            core.setLength(0);
            var db = new Database(core, opts);
            var rootCursor = db.rootCursor();

            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(new Database.Bytes("Hello, world!".getBytes()))
            });

            var cursor = rootCursor.readPath(new Database.PathPart[] {
                new Database.ArrayListGet(0)
            });
            var reader = cursor.getReader();
            var buffer = new byte[20];
            var size = reader.read(buffer);
            assertEquals("Hello, world!", new String(buffer, 0, size));
        }

        {
            core.setLength(0);
            var db = new Database(core, opts);
            var rootCursor = db.rootCursor();

            var fooHash = db.hasher.digest("foo".getBytes());
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooHash))
            });

            fooHash[fooHash.length-2] = 0;
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooHash))
            });
        }
    }
}