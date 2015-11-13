import java.sql.Connection;
import java.sql.SQLException;

public class Transactionally {
    public interface Action<T> {
        T run() throws SQLException;
    }

    public static <T> T run(Connection c, Action<T> action) throws SQLException {
        final boolean initialAutoCommit = c.getAutoCommit();
        if (!initialAutoCommit) {
            // Already in transaction, join that one
            return action.run();
        } else {
            c.setAutoCommit(false);

            boolean success = false;
            try {
                final T result = action.run();
                success = true;
                return result;
            } finally {
                if (!success) {
                    c.rollback();
                }
                c.setAutoCommit(true);
            }
        }
    }
}
