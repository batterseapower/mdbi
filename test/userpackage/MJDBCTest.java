package userpackage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.co.omegaprime.mdbi.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
import static uk.co.omegaprime.mdbi.MJDBC.sql;

public class MJDBCTest {
    private Context ctxt;
    private Connection conn;
    private MJDBC m;

    @Before
    public void setUp() throws SQLException {
        ctxt = Context.createDefault();
        conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        m = new MJDBC(ctxt, conn);

        m.execute(sql("create table person (id integer, name string)"));
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    public void simple() throws SQLException {
        m.execute(sql("insert into person (id, name) values (1, 'Max')"));
        m.execute(sql("insert into person (id, name) values (2, ").$("John").sql(")"));
        Assert.assertEquals(Collections.singletonList("Max"),
                m.queryList(sql("select name from person where id = 1"), String.class));
        Assert.assertEquals(Collections.singletonList("John"),
                m.queryList(sql("select name from person where id = 2"), String.class));
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
        m.execute(sql("insert into person (id, name) values (").$(1).sql(",").$("Max").sql(")"));
        final Row row = m.queryFirst(sql("select * from person"), Reads.tuple(Row.class));
        assertEquals(1, row.id);
        assertEquals("Max", row.name);
    }

    @Test
    public void tupleWrite() throws SQLException {
        ctxt.registerWrite(Row.class, TupleWriteBuilder.<Row>create()
                .add(int.class, r -> r.id)
                .add(String.class, r -> r.name)
                .build());
        m.execute(sql("insert into person (id, name) values (").$(new Row(1, "Max")).sql(")"));
        Assert.assertEquals("Max", m.queryFirst(sql("select name from person"), String.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void failIfClassUnregistered() throws SQLException {
        m.queryList(sql("select * from person"), Row.class);
    }

    @Test
    public void canRegisterClass() throws SQLException {
        ctxt.registerRead(Row.class, Reads.tuple(Row.class));

        m.execute(sql("insert into person (id, name) values (").$(1).sql(",").$("Max").sql(")"));
        final Row row = m.queryFirst(sql("select * from person"), Row.class);
        assertEquals("Max", row.name);
    }

    @Test
    public void matrix() throws SQLException {
        m.execute(sql("insert into person (id, name) values (").$(1).sql(",").$("Max") .sql(")"));
        m.execute(sql("insert into person (id, name) values (").$(2).sql(",").$("John").sql(")"));

        final Object[] matrix = m.query(sql("select id, name from person"), BatchReads.matrix(int.class, String.class));
        assertArrayEquals(new int[] { 1, 2 }, (int[])matrix[0]);
        assertArrayEquals(new String[] { "Max", "John" }, (String[])matrix[1]);
    }

    @Test
    public void unprepared() throws SQLException {
        // It's useful to have access to unprepared statements when working with e.g. MS SQL Server, where temp tables
        // are scoped to just the prepared statement that creates them. This means that it's impossible to use most
        // JDBC wrapper libs to create a temp table that will be visible to future queries executing against that
        // same connection. But you can do it with this wrapper lib!
        m.withPrepared(false).execute(sql("create temp table temp (name string)"));
        m.withPrepared(false).execute(sql("insert into temp (name) values (").$("O'bama").sql(")"));
        Assert.assertEquals("O'bama", m.withPrepared(false).queryFirst(sql("select name from temp"), String.class));
    }

    private void assertStringRoundtrips(String x) throws SQLException {
        Assert.assertEquals(x, m.withPrepared(false).queryFirst(sql("select ").$(x), String.class));
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
        Assert.assertEquals(0L, m.update(sql("update person set id = id + 1")));
        m.execute(sql("insert into person (id, name) values (").$(1).sql(",").$("Max").sql(")"));
        Assert.assertEquals(1L, m.update(sql("update person set id = id + 1")));
    }

    @Test
    public void unpreparedUpdate() throws SQLException {
        Assert.assertEquals(0L, m.update(sql("update person set id = id + 1")));
        m.withPrepared(false).execute(sql("insert into person (id, name) values (").$(1).sql(",").$("Max").sql(")"));
        Assert.assertEquals(1L, m.update(sql("update person set id = id + 1")));
    }

    @Test
    public void updateBatch() throws SQLException {
        final List<Integer> ids = Arrays.asList(1, 2);
        final List<String> names = Arrays.asList("Max", "John");
        m.updateBatch(sql("insert into person (id, name) values(").$s(ids).sql(", ").$s(names).sql(")"));
        Assert.assertEquals(Arrays.asList("1Max", "2John"), m.queryList(sql("select id || name from person order by id"), String.class));
    }

    @Test
    public void unpreparedUpdateBatch() throws SQLException {
        final List<Integer> ids = Arrays.asList(1, 2);
        final List<String> names = Arrays.asList("Max", "John");
        m.withPrepared(false).updateBatch(sql("insert into person (id, name) values(").$s(ids).sql(", ").$s(names).sql(")"));
        Assert.assertEquals(Arrays.asList("1Max", "2John"), m.queryList(sql("select id || name from person order by id"), String.class));
    }

    @Test
    public void updateBatchNoParams() throws SQLException {
        m.updateBatch(sql("insert into person (id, name) values(1, 'foo')"));
        Assert.assertEquals(0, m.queryFirst(sql("select count(*) from person"), int.class).intValue());
    }

    @Test
    public void localDate() throws SQLException {
        Assert.assertEquals(LocalDate.of(2015, 8, 1), m.queryFirst(sql("select ").$(LocalDate.of(2015, 8, 1)), LocalDate.class));
        Assert.assertEquals("2015-08-01", m.queryFirst(sql("select date('2015-08-01')"), String.class));
        Assert.assertEquals("2015-08-01", m.queryFirst(sql("select date(").$(LocalDate.of(2015, 8, 1)).sql(" / 1000, 'unixepoch')"), String.class));
    }

    @Test
    public void localTime() throws SQLException {
        Assert.assertEquals(LocalTime.of(13, 13), m.queryFirst(sql("select ").$(LocalTime.of(13, 13)), LocalTime.class));
        Assert.assertEquals("13:13:00", m.queryFirst(sql("select time('13:13')"), String.class));
        Assert.assertEquals("13:13:00", m.queryFirst(sql("select time(").$(LocalTime.of(13, 13)).sql(" / 1000, 'unixepoch')"), String.class));
    }

    @Test
    public void localDateTime() throws SQLException {
        final LocalDateTime ldt = LocalDateTime.of(2015, 8, 1, 2, 30, 44);
        Assert.assertEquals(ldt, m.queryFirst(sql("select ").$(ldt), LocalDateTime.class));
        Assert.assertEquals("2015-08-01 02:30:44", m.queryFirst(sql("select datetime('2015-08-01 02:30:44')"), String.class));
        Assert.assertEquals("2015-08-01 02:30:44", m.queryFirst(sql("select datetime(").$(ldt).sql(" / 1000, 'unixepoch')"), String.class));
    }

    @Test
    public void updateBatchColumnAllNulls() throws SQLException {
        final List<Integer> ids = Arrays.asList(1, 2);
        final List<String> names = Arrays.asList(null, null);
        m.updateBatch(sql("insert into person (id, name) values(").$s(ids).sql(", ").$s(names).sql(")"));
        Assert.assertEquals(Arrays.asList("1null", "2null"), m.queryList(sql("select id || ifnull(name, 'null') from person order by id"), String.class));
    }

    private void assertDoublesWork(boolean prepared) throws SQLException {
        assertEquals("1.2345678E7", Double.toString(12345678));
        Assert.assertEquals(12345678.0, m.withPrepared(prepared).queryFirst(sql("select ").$(12345678.0), double.class).doubleValue(), 0.00001);
        Assert.assertEquals(12345678.0, m.withPrepared(prepared).queryFirst(sql("select ").$(12345678.0), Double.class).doubleValue(), 0.00001);
        assertTrue(Double.isNaN(m.withPrepared(prepared).queryFirst(sql("select ").$(null), double.class).doubleValue()));
        Assert.assertEquals(null, m.withPrepared(prepared).queryFirst(sql("select ").$(null), Double.class));
        assertTrue(Double.isNaN(m.withPrepared(prepared).queryFirst(sql("select ").$(Double.NaN), double.class).doubleValue()));
        Assert.assertEquals(null, m.withPrepared(prepared).queryFirst(sql("select ").$(Double.NaN), Double.class));
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
        m.update(sql("insert into person (id, name) values(1, 'foo')"));
        final Bean bean = m.queryFirst(sql("select id, name from person"), Reads.bean(Bean.class, "Id", "Name"));
        assertEquals(1, bean.id);
        assertEquals("foo", bean.name);
    }

    @Test
    public void beanWrite() throws SQLException {
        ctxt.registerWrite(Bean.class, Writes.bean(Bean.class, "Id", "Name"));

        final Bean bean = new Bean();
        bean.id = 1;
        bean.name = "Max";
        m.update(sql("insert into person (id, name) values (").$(bean).sql(")"));
        Assert.assertEquals("Max", m.queryFirst(sql("select name from person"), String.class));
    }

    @Test
    public void in() throws SQLException {
        Assert.assertEquals(1, m.queryList(sql("select 1 where 1 ").in(1, 2), String.class).size());
        Assert.assertEquals(0, m.queryList(sql("select 1 where 1 ").in(2), String.class).size());
        Assert.assertEquals(0, m.queryList(sql("select 1 where 1 ").in(), String.class).size());
    }

    class Supertype {
        public final int x;

        public Supertype(int x) {
            this.x = x;
        }
    }

    class Subtype extends Supertype {
        public Subtype(int x) {
            super(-x);
        }
    }

    @Test
    public void variance() throws SQLException {
        // Write a subtype using a mapping designed for a supertype
        ctxt.registerWrite(Subtype.class, Writes.<Integer, Supertype>map(Writes.PRIM_INT, t -> t.x));

        // Read back a supertype using a mapping designed for a subtype
        ctxt.registerRead(Supertype.class, Reads.map(Subtype.class, Reads.PRIM_INT, Subtype::new));

        final Supertype result = m.queryFirst(sql("select ").$(new Subtype(1)), Supertype.class);
        assertTrue(result instanceof Subtype);
        assertEquals(1, result.x);
    }
}
