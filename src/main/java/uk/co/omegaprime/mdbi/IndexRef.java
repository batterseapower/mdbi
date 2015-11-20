package uk.co.omegaprime.mdbi;

/** Mutable type indicating which JDBC column is the next to be consumed. */
public class IndexRef {
    private int x = 1;

    /** Returns an instance where the first {@link #take()} will return 1, the first column index in any {@code ResultSet}. */
    public IndexRef() {}

    /** Returns the next column index to consume, and simultaneously advances the {@code IndexRef} to point to the next column. */
    public int take() {
        return x++;
    }
}
