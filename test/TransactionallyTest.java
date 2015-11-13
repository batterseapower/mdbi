import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransactionallyTest {
    private Connection conn;
    private MJDBC m;

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        m = new MJDBC(Context.createDefault(), conn);
        m.execute(SQL.of("create table tab (value integer)"));
        m.execute(SQL.of("insert into tab (value) values (0)"));
    }

    @Test
    public void transactionallySucceed() throws SQLException {
        assertEquals(0, Transactionally.run(conn, () -> m.queryExactlyOne(SQL.of("select value from tab"), int.class)).intValue());
        assertTrue(conn.getAutoCommit());
    }

    @Test
    public void transactionallySucceedNested() throws SQLException {
        assertEquals(0, Transactionally.run(conn, () -> Transactionally.run(conn, () -> m.queryExactlyOne(SQL.of("select value from tab"), int.class))).intValue());
        assertTrue(conn.getAutoCommit());
    }

    @Test
    public void transactionallyFail() throws SQLException {
        boolean thrown = false;
        try {
            Transactionally.run(conn, () -> {
                m.execute(SQL.of("update tab set value = value + 1"));
                m.execute(SQL.of("i'm a bogus query lol"));
                return null;
            });
        } catch (SQLException e) {
            if (!e.getMessage().contains("near \"i\"")) {
                throw e;
            } else {
                thrown = true;
            }
        }

        assertTrue(conn.getAutoCommit());
        assertTrue(thrown);
        assertEquals(0, m.queryExactlyOne(SQL.of("select value from tab"), int.class).intValue());
    }

    @Test
    public void transactionallyFailNested() throws SQLException {
        boolean thrown = false;
        try {
            Transactionally.run(conn, () -> {
                m.execute(SQL.of("update tab set value = value + 1"));
                return Transactionally.run(conn, () -> {
                    m.execute(SQL.of("update tab set value = value + 1"));
                    m.execute(SQL.of("i'm a bogus query lol"));
                    return null;
                });
            });
        } catch (SQLException e) {
            if (!e.getMessage().contains("near \"i\"")) {
                throw e;
            } else {
                thrown = true;
            }
        }

        assertTrue(conn.getAutoCommit());
        assertTrue(thrown);
        assertEquals(0, m.queryExactlyOne(SQL.of("select value from tab"), int.class).intValue());
    }
}
