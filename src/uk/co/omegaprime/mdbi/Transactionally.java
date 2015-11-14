package uk.co.omegaprime.mdbi;

import com.sun.media.jfxmedia.locator.ConnectionHolder;

import java.sql.Connection;
import java.sql.SQLException;

public class Transactionally {
    private Transactionally() {}

    private static class TransactionHelper implements AutoCloseable {
        private final Connection conn;
        private boolean success;

        public TransactionHelper(Connection conn) throws SQLException {
            this.conn = conn;
            conn.setAutoCommit(false);
        }

        @Override
        public void close() throws SQLException {
            if (!success) {
                conn.rollback();
            }
            conn.setAutoCommit(true);
        }
    }

    public static <T> T run(Connection c, SQLAction<T> SQLAction) throws SQLException {
        if (!c.getAutoCommit()) {
            // Already in transaction, join that one
            return SQLAction.run();
        } else {
            try (TransactionHelper th = new TransactionHelper(c)) {
                final T result = SQLAction.run();
                th.success = true;
                return result;
            }
        }
    }
}
