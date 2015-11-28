package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;

/** Functions for creating useful instances of {@link StatementlikeBatchRead}. */
@ParametersAreNonnullByDefault
public final class StatementlikeBatchReads {
    private StatementlikeBatchReads() {}

    /** Creates a {@code BatchRead} that just processes a {@code ResultSet} via the {@link BatchRead} interface. */
    public static <T> StatementlikeBatchRead<T> fromBatchRead(BatchRead<T> rsbr) {
        return (ctxt, ps) -> rsbr.get(ctxt, ps.executeQuery());
    }
}
