package userpackage;

import uk.co.omegaprime.mdbi.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static uk.co.omegaprime.mdbi.MDBI.sql;

public class Sample {
    public static void main(String[] args) throws SQLException {
        // Getting started with MDBI is easy: all you need is a javax.sql.DataSource or Connection
        final Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        MDBI.of(conn).execute(sql("create table people (name text, age int)"));

        // As well as executing raw SQL, it is of course possible to interpolate Java objects into the query
        final String name = "Michael O'Hare";
        MDBI.of(conn).execute(sql("insert into people (name, age) values (").$(name).sql(", 30)"));

        // To get back Java objects from the database, simply use a "query" method rather than "execute"
        final int age = MDBI.of(conn).queryFirst(sql("select age from people"), int.class);
        println(age); // 30

        // Most people who work with JDBC have been burned at some point by the fact that it silently
        // turns NULLs in the database into zeroes, which is almost never what you want. MDBI removes
        // this misfeature, so the following query actually throws a NullPointerException:
        //MDBI.of(conn).queryFirst(sql("select null"), int.class);

        // Of course, you can still retrieve nulls if you explicitly ask for them:
        final Integer nully = MDBI.of(conn).queryFirst(sql("select null"), Integer.class);
        println(nully); // null

        // Note that database NULLs *are* supported when retrieving primitive doubles and floats, where
        // they can be cleanly mapped to NaNs
        final double nullyDouble = MDBI.of(conn).queryFirst(sql("select null"), double.class);
        println(nullyDouble); // NaN

        // Batch insert is fully supported
        final List<String> names = Arrays.asList("Fry", "Leela");
        final List<Integer> ages = Arrays.asList(1025, 25);
        MDBI.of(conn).updateBatch(sql("insert into people (name, age) values (").$s(names).sql(",").$s(ages).sql(")"));

        // You can even mix batched and non-batched bits of the query:
        final List<String> moreNames = Arrays.asList("Foo", "Bar");
        final int anotherAge = 13;
        MDBI.of(conn).updateBatch(sql("insert into people (name, age) values (").$s(moreNames).sql(",").$(anotherAge).sql(")"));

        // MDBI has built-in support for IN clauses
        final List<Integer> foundAges = MDBI.of(conn).queryList(sql("select age from people where name ").in("Fry", "Foo"), int.class);
        println(foundAges); // [1025, 13]

        // These IN clauses work properly with empty argument lists, even if the database does not normally support
        // nullary IN clauses (most databases don't -- SQLite is the only one I know of that supports them)
        final int count = MDBI.of(conn).queryFirst(sql("select count(*) from people where name not ").in(), int.class);
        println(count); // 5

        // There is transaction support that's really easy to use (no messing around with the confusing setAutocommit interface)
        try {
            Transactionally.run(conn, () -> {
                MDBI.of(conn).execute(sql("insert into people (name, age) values ('foo', 1)"));
                throw new IllegalArgumentException("Changed my mind!");
            });
        } catch (IllegalArgumentException _) {}

        final int postTransactionCount = MDBI.of(conn).queryFirst(sql("select count(*) from people"), int.class);
        println(postTransactionCount); // 5

        // You can get structured types out of the database, not just primitives:
        final Map<String, Integer> ageMap = MDBI.of(conn).queryMap(sql("select name, age from people"), String.class, int.class);
        println(ageMap.get("Fry")); // 1025

        // One that is particularly handy is the "matrix":
        final Object[] matrix = MDBI.of(conn).query(sql("select name, age from people order by name"),
                                                    BatchReads.matrix(String.class, int.class));
        final String[] nameColumn = (String[])matrix[0];
        final int[] ageColumn = (int[])matrix[1];
        println(nameColumn[0] + ": " + ageColumn[1]); // Bar: 13

        // MDBI has great support for Java primitive types, but it can also be extended with support for your own. Let's say
        // you have a bean, PersonBean, representing one row of the table. This works:
        final Context ctxt0 = Context.Builder.createDefault()
                .registerRead(PersonBean.class, Reads.bean(PersonBean.class, "Name", "Age"))
                .build();
        final PersonBean bean = MDBI.of(ctxt0, conn).queryFirst(sql("select name, age from people order by name"), PersonBean.class);
        println(bean.getName()); // Bar

        // If you don't like beans, that's no problem. There are also strongly-typed interfaces suitable for immutable data types:
        //   public class Person {
        //    public final String name;
        //    public final int age;
        //
        //    public Person(String name, int age) {
        //      this.name = name;
        //      this.age = age;
        //    }
        //   }
        final Context ctxt1 = Context.Builder.createDefault()
                .registerRead(Person.class, Reads.tuple(Person.class))
                .build();
        final Person person = MDBI.of(ctxt1, conn).queryFirst(sql("select name, age from people order by name"), Person.class);
        println(person.name); // Bar

        // Custom types are also usable when you are constructing SQL queries -- you just use registerWrite instead of registerRead:
        final Context ctxt2 = Context.Builder.createDefault()
                .registerWrite(Person.class, TupleWriteBuilder.<Person>create()
                                                .add(String.class, p -> p.name)
                                                .add(int.class,    p -> p.age)
                                                .build())
                .build();
        final Person personToSave = new Person("Max", 29);
        MDBI.of(ctxt2, conn).execute(sql("insert into people (name, age) values (").$(personToSave).sql(")"));

        // And there are lots more features besides:
        //  * Deadlocks are automatically retried. (The retry policy is fully customizable.)
        //  * Java 8 date and time types are fully supported
        //  * Support for both PreparedStatement and Statement. This can be useful when working with a database
        //    that e.g. scopes the lifetime of temp tables to a prepared statement.
        // All of this comes with no runtime dependencies at all -- you only need the JDK.
    }

    public static void println(Object x) {
        System.out.println(x);
    }

    public static class Person {
        public final String name;
        public final int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    public static class PersonBean {
        private String name;
        private int age;

        public String getName() { return name; }
        public int    getAge()  { return age; }

        public void setName(String name) { this.name = name; }
        public void setAge (int age)     { this.age = age; }
    }
}
