package userpackage;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.co.omegaprime.mdbi.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.omegaprime.mdbi.MDBI.sql;

public class TransactionallyTest {
    private Connection conn;
    private MDBI m;

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        m = MDBI.of(conn);
        m.execute(sql("create table tab (value integer)"));
        m.execute(sql("insert into tab (value) values (0)"));
    }

    @Test
    public void transactionallySucceed() throws SQLException {
        Assert.assertEquals(0, Transactionally.run(conn, () -> m.queryFirst(sql("select value from tab"), int.class)).intValue());
        assertTrue(conn.getAutoCommit());
    }

    @Test
    public void transactionallySucceedNested() throws SQLException {
        Assert.assertEquals(0, Transactionally.run(conn, () -> Transactionally.run(conn, () -> m.queryFirst(sql("select value from tab"), int.class))).intValue());
        assertTrue(conn.getAutoCommit());
    }

    @Test
    public void transactionallyFail() throws SQLException {
        boolean thrown = false;
        try {
            Transactionally.run(conn, () -> {
                m.execute(sql("update tab set value = value + 1"));
                m.execute(sql("i'm a bogus query lol"));
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
        Assert.assertEquals(0, m.queryFirst(sql("select value from tab"), int.class).intValue());
    }

    @Test
    public void transactionallyFailNested() throws SQLException {
        boolean thrown = false;
        try {
            Transactionally.run(conn, () -> {
                m.execute(sql("update tab set value = value + 1"));
                return Transactionally.run(conn, () -> {
                    m.execute(sql("update tab set value = value + 1"));
                    m.execute(sql("i'm a bogus query lol"));
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
        Assert.assertEquals(0, m.queryFirst(sql("select value from tab"), int.class).intValue());
    }
}
