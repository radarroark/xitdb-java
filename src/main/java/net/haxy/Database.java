package net.haxy;

public class Database {
    Kind kind;

    public Database(Kind kind) {
        this.kind = kind;
    }

    public enum Kind {
        FILE
    }
}