import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class MJDBCTest {
    private Context ctxt;
    private Connection conn;
    private MJDBC m;

    @Before
    public void setUp() throws SQLException {
        ctxt = Context.createDefault();
        conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        m = new MJDBC(ctxt, conn);
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    public void simple() throws SQLException {
        m.execute(SQL.of("create table person (id integer, name string)"));
        m.execute(SQL.of("insert into person (id, name) values (1, 'Max')"));
        m.execute(SQL.of("insert into person (id, name) values (2, ", "John", ")"));
        assertEquals(Collections.singletonList("Max"),
                     m.queryList(SQL.of("select name from person where id = 1"), String.class));
        assertEquals(Collections.singletonList("John"),
                     m.queryList(SQL.of("select name from person where id = 2"), String.class));
    }
}
