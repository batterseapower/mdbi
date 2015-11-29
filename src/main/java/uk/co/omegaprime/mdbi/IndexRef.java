package uk.co.omegaprime.mdbi;

/** Mutable type indicating which JDBC column is the next to be consumed. */
public class IndexRef {
    private int x = 1;

    private IndexRef() {}

    /** Returns an instance where the first {@link #take()} will return 1, the first column index in any {@code ResultSet}. */
    public static IndexRef create() {
        return new IndexRef();
    }

    /** Returns the next column index to consume, and simultaneously advances the {@code IndexRef} to point to the next column. */
    public int take() {
        return x++;
    }

    /** Returns the next item that {@link #take()} would return, without actually advancing the index. */
    public int peek() { return x; }
}
