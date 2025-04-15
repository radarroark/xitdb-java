package net.haxy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {
    @Test
    void initDatabase() {
        var db = new Database(Database.Kind.FILE);
        assertEquals(Database.Kind.FILE, db.kind);
    }
}
