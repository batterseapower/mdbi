import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
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

        m.execute(SQL.of("create table person (id integer, name string)"));
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    public void simple() throws SQLException {
        m.execute(SQL.of("insert into person (id, name) values (1, 'Max')"));
        m.execute(SQL.of("insert into person (id, name) values (2, ", "John", ")"));
        assertEquals(Collections.singletonList("Max"),
                     m.queryList(SQL.of("select name from person where id = 1"), String.class));
        assertEquals(Collections.singletonList("John"),
                     m.queryList(SQL.of("select name from person where id = 2"), String.class));
    }

    public static class Row {
        public final int id;
        public final String name;

        public Row(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    @Test
    public void tuple() throws SQLException {
        m.execute(SQL.of("insert into person (id, name) values (", 1, ",", "Max", ")"));
        final Row row = m.queryExactlyOne(SQL.of("select * from person"), new TupleRead<>(Row.class));
        assertEquals(1, row.id);
        assertEquals("Max", row.name);
    }

    @Test(expected = IllegalArgumentException.class)
    public void failIfClassUnregistered() throws SQLException {
        m.queryList(SQL.of("select * from person"), Row.class);
    }

    @Test
    public void canRegisterClass() throws SQLException {
        ctxt.registerRead(Row.class, new TupleRead<>(Row.class));
        m.execute(SQL.of("insert into person (id, name) values (", 1, ",", "Max", ")"));
        final Row row = m.queryExactlyOne(SQL.of("select * from person"), Row.class);
        assertEquals("Max", row.name);
    }

    @Test
    public void matrix() throws SQLException {
        m.execute(SQL.of("insert into person (id, name) values (", 1, ",", "Max", ")"));
        m.execute(SQL.of("insert into person (id, name) values (", 2, ",", "John", ")"));

        final Object[] matrix = m.query(SQL.of("select id, name from person"), new MatrixBatchRead(int.class, String.class));
        assertArrayEquals(new int[] { 1, 2 }, (int[])matrix[0]);
        assertArrayEquals(new String[] { "Max", "John" }, (String[])matrix[1]);
    }
}
