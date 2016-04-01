package userpackage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.co.omegaprime.mdbi.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static uk.co.omegaprime.mdbi.MDBI.$;
import static uk.co.omegaprime.mdbi.MDBI.$s;
import static uk.co.omegaprime.mdbi.MDBI.sql;

public class MDBITest {
    private Connection conn;
    private MDBI m;

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        m = MDBI.of(conn);

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
        m.execute(sql("insert into person (id, name) values (2, ", $("John"), ")"));
        Assert.assertEquals(Collections.singletonList("Max"),
                m.queryList(sql("select name from person where id = 1"), String.class));
        Assert.assertEquals(Arrays.asList("John", "John"),
                m.queryList(sql("select name from person where id = 2"), String.class));
    }

    // Intentionally private to check that MDBI can still invoke the constructor via reflection
    private static class Row {
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
        final Context ctxt = Context.Builder.createDefault()
                .registerWrite(Row.class, TupleWriteBuilder.<Row>create()
                                                           .add(int.class, r -> r.id)
                                                           .add(String.class, r -> r.name)
                                                           .build())
                .build();
        MDBI.of(ctxt, conn).execute(sql("insert into person (id, name) values (").$(new Row(1, "Max")).sql(")"));
        Assert.assertEquals("Max", m.queryFirst(sql("select name from person"), String.class));
    }

    public static class TwoConstructors {
        public final int id;
        public final String name;

        public TwoConstructors(int id, int name) {
            this(id, "Via Ints " + Integer.toString(name));
        }

        public TwoConstructors(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    @Test
    public void tupleReadAmbiguousConstructor1() throws SQLException {
        final Context ctxt = Context.Builder.createDefault()
                .registerRead(TwoConstructors.class, Reads.tupleWithFieldClasses(TwoConstructors.class, Arrays.asList(int.class, int.class)))
                .build();

        final TwoConstructors result = MDBI.of(ctxt, conn).queryFirst(sql("select 1, 1"), TwoConstructors.class);
        assertEquals("Via Ints 1", result.name);
    }

    @Test
    public void tupleReadAmbiguousConstructor2() throws SQLException {
        final Context ctxt = Context.Builder.createDefault()
                .registerRead(TwoConstructors.class, Reads.tupleWithFieldClasses(TwoConstructors.class, Arrays.asList(int.class, String.class)))
                .build();

        final TwoConstructors result = MDBI.of(ctxt, conn).queryFirst(sql("select 1, 1"), TwoConstructors.class);
        assertEquals("1", result.name);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tupleReadAmbiguousConstructorsUnresolvable() throws SQLException {
        Reads.tuple(TwoConstructors.class);
    }

    public static class ConstructorSubtyping {
        public ConstructorSubtyping(CharSequence cs) {}
    }

    @Test
    public void tupleReadSubtyping() throws SQLException {
        final Context ctxt = Context.Builder.createDefault()
                .registerRead(ConstructorSubtyping.class, Reads.tupleWithFieldClasses(ConstructorSubtyping.class, Arrays.asList(String.class)))
                .build();

        final ConstructorSubtyping result = MDBI.of(ctxt, conn).queryFirst(sql("select 'hello'"), ConstructorSubtyping.class);
        assertNotNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void failIfClassUnregistered() throws SQLException {
        m.queryList(sql("select * from person"), Row.class);
    }

    @Test
    public void canRegisterClass() throws SQLException {
        final Context ctxt = Context.Builder.createDefault().registerRead(Row.class, Reads.tuple(Row.class)).build();

        MDBI.of(ctxt, conn).execute(sql("insert into person (id, name) values (").$(1).sql(",").$("Max").sql(")"));
        final Row row = MDBI.of(ctxt, conn).queryFirst(sql("select * from person"), Row.class);
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

        m.execute(sql("delete from person"));

        // In an unlikely twist we had a bug where this way of constructing the SQL statement didn't work
        m.updateBatch(sql("insert into person (id, name) values(", $s(ids), ", ", $s(names), ")"));
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

    private void assertBigDecimalsWork(boolean prepared) throws SQLException {
        Assert.assertEquals(new BigDecimal(12345678), m.withPrepared(prepared).queryFirst(sql("select ").$(new BigDecimal(12345678)), BigDecimal.class));
        Assert.assertEquals(new BigDecimal("1234.5678"), m.withPrepared(prepared).queryFirst(sql("select ").$(new BigDecimal("1234.5678")), BigDecimal.class));
        Assert.assertEquals(new BigDecimal("0.123456789"), m.withPrepared(prepared).queryFirst(sql("select ").$(new BigDecimal("0.123456789")), BigDecimal.class));
        assertNull(m.withPrepared(prepared).queryFirst(sql("select ").$(null), BigDecimal.class));
    }

    @Test
    public void bigDecimals() throws SQLException {
        assertBigDecimalsWork(false);
        assertBigDecimalsWork(true);
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
        final Context ctxt = Context.Builder.createDefault().registerWrite(Bean.class, Writes.bean(Bean.class, "Id", "Name")).build();

        final Bean bean = new Bean();
        bean.id = 1;
        bean.name = "Max";
        MDBI.of(ctxt, conn).update(sql("insert into person (id, name) values (").$(bean).sql(")"));
        Assert.assertEquals("Max", m.queryFirst(sql("select name from person"), String.class));
    }

    @Test
    public void in() throws SQLException {
        Assert.assertEquals(1, m.queryList(sql("select 1 where 1 ").in(1, 2), String.class).size());
        Assert.assertEquals(0, m.queryList(sql("select 1 where 1 ").in(2), String.class).size());
        Assert.assertEquals(1, m.queryList(sql("select 1 where 1 not ").in(2), String.class).size());
        Assert.assertEquals(0, m.queryList(sql("select 1 where 1 ").in(), String.class).size());
        Assert.assertEquals(1, m.queryList(sql("select 1 where 1 not ").in(), String.class).size());
        Assert.assertEquals(1, m.queryList(sql("select 1 where 1").in(1).sql("and 1=1"), String.class).size());
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
    public void contextLookupVariance() throws SQLException {
        final Context ctxt = Context.Builder.createDefault()
            // Write a subtype using a mapping designed for a supertype
            .registerWrite(Subtype.class, Writes.<Integer, Supertype>map(Writes.PRIM_INT, t -> t.x))
            // Read back a supertype using a mapping designed for a subtype
            .registerRead(Supertype.class, Reads.map(Subtype.class, Reads.PRIM_INT, Subtype::new))
            .build();

        final Supertype result = MDBI.of(ctxt, conn).queryFirst(sql("select ").$(new Subtype(1)), Supertype.class);
        assertTrue(result instanceof Subtype);
        assertEquals(1, result.x);
    }

    @Test
    public void writeReadList() throws SQLException {
        final Write<List<Object>> write = Writes.listWithClasses(Arrays.<Class<?>>asList(Integer.class, String.class));
        MDBI.of(conn).execute(sql("insert into person (id, name) values (").$(write, Arrays.asList(1, "Moomin")).sql(")"));

        final Read<List<Object>> read = Reads.listWithClasses(Arrays.<Class<?>>asList(Integer.class, String.class));
        final List<Object> result = MDBI.of(conn).queryFirst(sql("select id, name from person where name = 'Moomin'"), read);
        assertEquals(result, Arrays.asList(1, "Moomin"));
    }

    @Test
    public void readLabelledMap() throws SQLException {
        MDBI.of(conn).execute(sql("insert into person (id, name) values (1, 'Moomin')"));

        final Read<Map<String, Object>> read = Reads.labelledMapWithClasses(Arrays.<Class<?>>asList(Integer.class, String.class));
        final Map<String, Object> result = MDBI.of(conn).queryFirst(sql("select id, name from person where name = 'Moomin'"), read);
        assertEquals(2, result.size());
        assertEquals(1, result.get("id"));
        assertEquals("Moomin", result.get("name"));
    }

    @Test
    public void queryMapViaSegmented() throws SQLException {
        MDBI.of(conn).execute(sql("insert into person (id, name) values (1, 'Foo')"));
        MDBI.of(conn).execute(sql("insert into person (id, name) values (2, 'Bar')"));

        final Map<Integer, String> simple = MDBI.of(conn).query(sql("select id, name from person"),
                BatchReads.asMap(int.class, String.class));
        final Map<Integer, String> complex = MDBI.of(conn).query(sql("select id, name from person"),
                BatchReads.asMap(Reads.useContext(int.class), BatchReads.first(String.class)));

        assertEquals(simple, complex);
    }

    @Test
    public void queryMultiMapViaSegmented() throws SQLException {
        MDBI.of(conn).execute(sql("insert into person (id, name) values (1, 'Foo')"));
        MDBI.of(conn).execute(sql("insert into person (id, name) values (1, 'Bar')"));
        MDBI.of(conn).execute(sql("insert into person (id, name) values (2, 'Baz')"));

        final Map<Integer, List<String>> simple = MDBI.of(conn).query(sql("select id, name from person order by id"),
                BatchReads.asMultiMap(int.class, String.class));
        final Map<Integer, List<String>> complex = MDBI.of(conn).query(sql("select id, name from person order by id"),
                BatchReads.asMap(Reads.useContext(int.class), BatchReads.asList(String.class)));

        assertEquals(simple, complex);
    }

    @Test
    public void queryMapFirstViaSegmented() throws SQLException {
        MDBI.of(conn).execute(sql("insert into person (id, name) values (1, 'Foo')"));
        MDBI.of(conn).execute(sql("insert into person (id, name) values (1, 'Bar')"));
        MDBI.of(conn).execute(sql("insert into person (id, name) values (2, 'Baz')"));

        final Map<Integer, String> simple = MDBI.of(conn).query(sql("select id, name from person order by id"),
                BatchReads.asMapFirst(int.class, String.class));
        final Map<Integer, String> complex = MDBI.of(conn).query(sql("select id, name from person order by id"),
                BatchReads.asMapFirst(Reads.useContext(int.class), BatchReads.first(String.class)));

        assertEquals(simple, complex);
    }

    @Test
    public void queryMultiMapSegmentedZeroRowTrickster() throws SQLException {
        MDBI.of(conn).execute(sql("insert into person (id, name) values (1, 'Foo')"));

        // I'm not 100% sure why this behaviour would be useful, but it is permitted by the interface...
        final boolean[] skip = new boolean[] { true };
        final Map<Integer, List<String>> result = MDBI.of(conn).query(sql("select id, name from person"),
                BatchReads.asMultiMap(Reads.useContext(int.class), new BatchRead<String>() {
                    @Override
                    public String get(Read.Context ctxt, ResultSet rs) throws SQLException {
                        if (skip[0]) {
                            skip[0] = false;
                            return "Bogus";
                        } else {
                            if (!rs.next()) throw new IllegalStateException();
                            return rs.getString(1);
                        }
                    }
                }));
        assertEquals(new HashMap<Integer, List<String>>() {{ put(1, Arrays.asList("Bogus", "Foo")); }}, result);
    }

    @Test
    public void queryMapMatrix() throws SQLException {
        final Map<Integer, Object[]> partedMatrix =
            MDBI.of(conn).query(sql("select 1, 2, 'Hello' union select 1, 3, 'World' union select 4, 5, 'Bags'"),
                BatchReads.asMap(Reads.useContext(int.class), BatchReads.matrix(int.class, String.class)));
        assertEquals(new HashSet<Integer>() {{ add(1); add(4); }}, partedMatrix.keySet());

        final Object[] matrix1 = partedMatrix.get(1);
        assertArrayEquals((int[])matrix1[0], new int[] { 2, 3 });
        assertArrayEquals((String[])matrix1[1], new String[] { "Hello", "World" });

        final Object[] matrix4 = partedMatrix.get(4);
        assertArrayEquals((int[])matrix4[0], new int[] { 5 });
        assertArrayEquals((String[])matrix4[1], new String[] { "Bags" });
    }

    @Test
    public void queryLabelledMatrix() throws SQLException {
        MDBI.of(conn).execute(sql("insert into person (id, name) values (-1, 'Bob')"));

        final Map<String, Object> matrix = MDBI.of(conn).query(sql("select id, name from person"), BatchReads.labelledMatrix(int.class, String.class));
        assertArrayEquals(new int[] { -1 }, (int[])matrix.get("id"));
        assertArrayEquals(new String[] { "Bob" }, (String[])matrix.get("name"));
    }

    private static class StringyType {
        @Override
        public String toString() {
            return "StringyType";
        }
    }

    @Test
    public void helpfulToString() {
        assertEquals("select 1 from foo where x = 1 and y = 'Brendan'",
                     sql("select 1 from foo where x = ").$(1).sql(" and y = ").$("Brendan").toString());

        assertEquals("select 1 from foo where x = 1 and y = ${StringyType}",
                     sql("select 1 from foo where x = ").$(1).sql(" and y = ").$(new StringyType()).toString());

        assertEquals("select 1 from foo where x = 1 and y = 'Brendan'\n" +
                     "select 1 from foo where x = 1 and y = 'John'",
                     sql("select 1 from foo where x = ").$(1).sql(" and y = ").$s(Arrays.asList("Brendan", "John")).toString());

        assertEquals("select 1 from foo where x = 1 and y = ${StringyType}\n" +
                        "select 1 from foo where x = 1 and y = ${StringyType}",
                sql("select 1 from foo where x = ").$(1).sql(" and y = ").$s(Arrays.asList(new StringyType(), new StringyType())).toString());
    }

    @Test
    public void matrixBatchReadBuilderTest() throws SQLException {
        final MatrixBatchReadBuilder mrb = MatrixBatchReadBuilder.create();
        final Supplier<int[]> ids = mrb.addInt(sql("id"));
        final Supplier<String[]> names = mrb.add(sql("name"), String.class);

        m.execute(sql("insert into person (id, name) values (1, 'Max')"));
        m.execute(sql("insert into person (id, name) values (2, 'John')"));

        final int n = mrb.buildAndExecute(m, columns -> sql("select ", columns, " from person order by id"));
        assertEquals(2, n);

        assertArrayEquals(new int[] { 1, 2 }, ids.get());
        assertArrayEquals(new String[] { "Max", "John" }, names.get());
    }

    @Test
    public void rowReadBuilderTest() throws SQLException {
        final RowReadBuilder lrb = RowReadBuilder.create();
        final IntSupplier id = lrb.addInt(sql("id"));
        final Supplier<String> name = lrb.add(sql("name"), String.class);

        m.execute(sql("insert into person (id, name) values (1, 'Max')"));
        m.execute(sql("insert into person (id, name) values (2, 'John')"));

        final List<List<Object>> rows = m.queryList(sql("select ", lrb.buildColumns(), " from person order by id"), lrb.build());
        assertEquals(2, rows.size());

        lrb.bindSuppliers(rows.get(0));
        assertEquals(1, id.getAsInt());
        assertEquals("Max", name.get());

        lrb.bindSuppliers(rows.get(1));
        assertEquals(2, id.getAsInt());
        assertEquals("John", name.get());
    }

    private enum Person { ENUM_PERSON_1, ENUM_PERSON_2 }

    @Test
    public void enumAsString() throws SQLException {
        m.execute(sql("insert into person (id, name) values (1, ", $(Writes.enumAsString(), Person.ENUM_PERSON_1), ")"));
        assertEquals(Person.ENUM_PERSON_1, m.queryFirst(sql("select name from person"), Reads.enumAsString(Person.class)));
    }

    @Test
    public void enumAsOrdinal() throws SQLException {
        m.execute(sql("insert into person (name, id) values ('Max', ", $(Writes.enumAsOrdinal(), Person.ENUM_PERSON_2), ")"));
        assertEquals(Integer.valueOf(1), m.queryFirst(sql("select id from person"), Reads.INTEGER));
        assertEquals(Person.ENUM_PERSON_2, m.queryFirst(sql("select id from person"), Reads.enumAsOrdinal(Person.class)));
    }

    @Test
    public void readFunction() throws SQLException {
        m.execute(sql("insert into person (id, name) values (3, 'John')"));

        assertEquals("John has 3 bottles of beer", m.queryFirst(sql("select id, name from person"), Reads.ofFunction(new Object() {
            public String f(int id, String name) { return name + " has " + id + " bottles of beer"; }
        })));

        assertEquals("John has 3 bottles of beer", m.queryFirst(sql("select id, name from person"), Reads.ofFunction(String.class, new Object() {
            public String f(int id, String name) { return name + " has " + id + " bottles of beer"; }
        })));

        assertEquals("John has 3 bottles of beer", m.queryFirst(sql("select id, name from person"), Reads.ofFunction(Object.class, new Object() {
            public String f(int id, String name) { return name + " has " + id + " bottles of beer"; }
        })));
    }
}
