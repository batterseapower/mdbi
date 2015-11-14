package userpackage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.co.omegaprime.mdbi.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        Assert.assertEquals(Collections.singletonList("Max"),
                m.queryList(SQL.of("select name from person where id = 1"), String.class));
        Assert.assertEquals(Collections.singletonList("John"),
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
    public void tupleRead() throws SQLException {
        m.execute(SQL.of("insert into person (id, name) values (", 1, ",", "Max", ")"));
        final Row row = m.queryFirst(SQL.of("select * from person"), Reads.tuple(Row.class));
        assertEquals(1, row.id);
        assertEquals("Max", row.name);
    }

    @Test
    public void tupleWrite() throws SQLException {
        ctxt.registerWrite(Row.class, TupleWriteBuilder.<Row>create()
                                                    .add(int.class, r -> r.id)
                                                    .add(String.class, r -> r.name)
                                                    .build());
        m.execute(SQL.of("insert into person (id, name) values (", new Row(1, "Max"), ")"));
        Assert.assertEquals("Max", m.queryFirst(SQL.of("select name from person"), String.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void failIfClassUnregistered() throws SQLException {
        m.queryList(SQL.of("select * from person"), Row.class);
    }

    @Test
    public void canRegisterClass() throws SQLException {
        ctxt.registerRead(Row.class, Reads.tuple(Row.class));

        m.execute(SQL.of("insert into person (id, name) values (", 1, ",", "Max", ")"));
        final Row row = m.queryFirst(SQL.of("select * from person"), Row.class);
        assertEquals("Max", row.name);
    }

    @Test
    public void matrix() throws SQLException {
        m.execute(SQL.of("insert into person (id, name) values (", 1, ",", "Max", ")"));
        m.execute(SQL.of("insert into person (id, name) values (", 2, ",", "John", ")"));

        final Object[] matrix = m.query(SQL.of("select id, name from person"), BatchReads.matrix(int.class, String.class));
        assertArrayEquals(new int[] { 1, 2 }, (int[])matrix[0]);
        assertArrayEquals(new String[] { "Max", "John" }, (String[])matrix[1]);
    }

    @Test
    public void unprepared() throws SQLException {
        // It's useful to have access to unprepared statements when working with e.g. MS SQL Server, where temp tables
        // are scoped to just the prepared statement that creates them. This means that it's impossible to use most
        // JDBC wrapper libs to create a temp table that will be visible to future queries executing against that
        // same connection. But you can do it with this wrapper lib!
        m.withPrepared(false).execute(SQL.of("create temp table temp (name string)"));
        m.withPrepared(false).execute(SQL.of("insert into temp (name) values (", "O'bama", ")"));
        Assert.assertEquals("O'bama", m.withPrepared(false).queryFirst(SQL.of("select name from temp"), String.class));
    }

    private void assertStringRoundtrips(String x) throws SQLException {
        Assert.assertEquals(x, m.withPrepared(false).queryFirst(SQL.of("select", x), String.class));
    }

    @Test
    public void trickyUnpreparedStringEscapes() throws SQLException {
        assertStringRoundtrips("foo\n\tbar");
        assertStringRoundtrips("foo\0bar");
        assertStringRoundtrips("foo\'\"\\bar");
        assertStringRoundtrips("foo\u001Abar");
        assertStringRoundtrips("你好");
    }

    @Test
    public void update() throws SQLException {
        Assert.assertEquals(0L, m.update(SQL.of("update person set id = id + 1")));
        m.execute(SQL.of("insert into person (id, name) values (", 1, ",", "Max", ")"));
        Assert.assertEquals(1L, m.update(SQL.of("update person set id = id + 1")));
    }

    @Test
    public void unpreparedUpdate() throws SQLException {
        Assert.assertEquals(0L, m.update(SQL.of("update person set id = id + 1")));
        m.withPrepared(false).execute(SQL.of("insert into person (id, name) values (", 1, ",", "Max", ")"));
        Assert.assertEquals(1L, m.update(SQL.of("update person set id = id + 1")));
    }

    @Test
    public void updateBatch() throws SQLException {
        final List<Integer> ids = Arrays.asList(1, 2);
        final List<String> names = Arrays.asList("Max", "John");
        m.updateBatch(SQL.of("insert into person (id, name) values(", ids, ", ", names, ")"));
        Assert.assertEquals(Arrays.asList("1Max", "2John"), m.queryList(SQL.of("select id || name from person order by id"), String.class));
    }

    @Test
    public void unpreparedUpdateBatch() throws SQLException {
        final List<Integer> ids = Arrays.asList(1, 2);
        final List<String> names = Arrays.asList("Max", "John");
        m.withPrepared(false).updateBatch(SQL.of("insert into person (id, name) values(", ids, ", ", names, ")"));
        Assert.assertEquals(Arrays.asList("1Max", "2John"), m.queryList(SQL.of("select id || name from person order by id"), String.class));
    }

    @Test
    public void updateBatchNoParams() throws SQLException {
        m.updateBatch(SQL.of("insert into person (id, name) values(1, 'foo')"));
        Assert.assertEquals(0, m.queryFirst(SQL.of("select count(*) from person"), int.class).intValue());
    }

    @Test
    public void localDate() throws SQLException {
        Assert.assertEquals(LocalDate.of(2015, 8, 1), m.queryFirst(SQL.of("select ", LocalDate.of(2015, 8, 1)), LocalDate.class));
        Assert.assertEquals("2015-08-01", m.queryFirst(SQL.of("select date('2015-08-01')"), String.class));
        Assert.assertEquals("2015-08-01", m.queryFirst(SQL.of("select date(", LocalDate.of(2015, 8, 1), " / 1000, 'unixepoch')"), String.class));
    }

    @Test
    public void localTime() throws SQLException {
        Assert.assertEquals(LocalTime.of(13, 13), m.queryFirst(SQL.of("select ", LocalTime.of(13, 13)), LocalTime.class));
        Assert.assertEquals("13:13:00", m.queryFirst(SQL.of("select time('13:13')"), String.class));
        Assert.assertEquals("13:13:00", m.queryFirst(SQL.of("select time(", LocalTime.of(13, 13), " / 1000, 'unixepoch')"), String.class));
    }

    @Test
    public void localDateTime() throws SQLException {
        final LocalDateTime ldt = LocalDateTime.of(2015, 8, 1, 2, 30, 44);
        Assert.assertEquals(ldt, m.queryFirst(SQL.of("select ", ldt), LocalDateTime.class));
        Assert.assertEquals("2015-08-01 02:30:44", m.queryFirst(SQL.of("select datetime('2015-08-01 02:30:44')"), String.class));
        Assert.assertEquals("2015-08-01 02:30:44", m.queryFirst(SQL.of("select datetime(", ldt, " / 1000, 'unixepoch')"), String.class));
    }

    @Test
    public void updateBatchColumnAllNulls() throws SQLException {
        final List<Integer> ids = Arrays.asList(1, 2);
        final List<String> names = Arrays.asList(null, null);
        m.updateBatch(SQL.of("insert into person (id, name) values(", ids, ", ", names, ")"));
        Assert.assertEquals(Arrays.asList("1null", "2null"), m.queryList(SQL.of("select id || ifnull(name, 'null') from person order by id"), String.class));
    }

    private void assertDoublesWork(boolean prepared) throws SQLException {
        assertEquals("1.2345678E7", Double.toString(12345678));
        Assert.assertEquals(12345678.0, m.withPrepared(prepared).queryFirst(SQL.of("SELECT ", 12345678.0), double.class).doubleValue(), 0.00001);
        Assert.assertEquals(12345678.0, m.withPrepared(prepared).queryFirst(SQL.of("SELECT ", 12345678.0), Double.class).doubleValue(), 0.00001);
        assertTrue(Double.isNaN(m.withPrepared(prepared).queryFirst(SQL.of("SELECT ", null), double.class).doubleValue()));
        Assert.assertEquals(null, m.withPrepared(prepared).queryFirst(SQL.of("SELECT ", null), Double.class));
        assertTrue(Double.isNaN(m.withPrepared(prepared).queryFirst(SQL.of("SELECT ", Double.NaN), double.class).doubleValue()));
        Assert.assertEquals(null, m.withPrepared(prepared).queryFirst(SQL.of("SELECT ", Double.NaN), Double.class));
    }

    @Test
    public void doubles() throws SQLException {
        assertDoublesWork(false);
        assertDoublesWork(true);
    }

    public static class Bean {
        private int id;
        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @Test
    public void beanRead() throws SQLException {
        m.update(SQL.of("insert into person (id, name) values(1, 'foo')"));
        final Bean bean = m.queryFirst(SQL.of("select id, name from person"), Reads.bean(Bean.class, "Id", "Name"));
        assertEquals(1, bean.id);
        assertEquals("foo", bean.name);
    }

    @Test
    public void beanWrite() throws SQLException {
        ctxt.registerWrite(Bean.class, Writes.bean(Bean.class, "Id", "Name"));

        final Bean bean = new Bean();
        bean.id = 1;
        bean.name = "Max";
        m.update(SQL.of("insert into person (id, name) values (", bean, ")"));
        Assert.assertEquals("Max", m.queryFirst(SQL.of("select name from person"), String.class));
    }

    @Test
    public void in() throws SQLException {
        Assert.assertEquals(1, m.queryList(SQL.of("select 1 where 1 in ", In.of(1, 2)), String.class).size());
        Assert.assertEquals(0, m.queryList(SQL.of("select 1 where 1 in ", In.of(2)), String.class).size());
        Assert.assertEquals(0, m.queryList(SQL.of("select 1 where 1 in ", In.of()), String.class).size());
    }
}
