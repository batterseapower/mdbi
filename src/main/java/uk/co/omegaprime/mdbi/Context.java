package uk.co.omegaprime.mdbi;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;

/**
 * An immutable object describing how to map Java types to SQL ones, and vice versa.
 * <p>
 * In order to construct one of these, you probably want to either just use {@link #DEFAULT}
 * or (if you want some customization), to use {@link Context.Builder}.
 */
public class Context {
    public static final Context DEFAULT = Builder.createDefault().build();

    /** A mutable builder allowing you to construct an immutable {@link Context} object. */
    public static class Builder {
        private final Reads.Map readers;
        private final Writes.Map writers;

        /** Returns a builder with no type mappings whatsoever &mdash; not even the built-in ones. */
        public static Builder createEmpty() {
            return new Builder(new Reads.Map(), new Writes.Map());
        }

        /** Returns a builder with the default set of type mappings, covering basic JDK types. */
        public static Builder createDefault() {
            final Builder context = createEmpty();
            context.register(boolean.class,       Writes.PRIM_BOOLEAN,    Reads.PRIM_BOOLEAN);
            context.register(Boolean.class,       Writes.BOOLEAN,         Reads.BOOLEAN);
            context.register(byte.class,          Writes.PRIM_BYTE,       Reads.PRIM_BYTE);
            context.register(Byte.class,          Writes.BYTE,            Reads.BYTE);
            context.register(char.class,          Writes.PRIM_CHAR,       Reads.PRIM_CHAR);
            context.register(Character.class,     Writes.CHARACTER,       Reads.CHARACTER);
            context.register(short.class,         Writes.PRIM_SHORT,      Reads.PRIM_SHORT);
            context.register(Short.class,         Writes.SHORT,           Reads.SHORT);
            context.register(int.class,           Writes.PRIM_INT,        Reads.PRIM_INT);
            context.register(Integer.class,       Writes.INTEGER,         Reads.INTEGER);
            context.register(long.class,          Writes.PRIM_LONG,       Reads.PRIM_LONG);
            context.register(Long.class,          Writes.LONG,            Reads.LONG);
            context.register(float.class,         Writes.PRIM_FLOAT,      Reads.PRIM_FLOAT);
            context.register(Float.class,         Writes.FLOAT,           Reads.FLOAT);
            context.register(double.class,        Writes.PRIM_DOUBLE,     Reads.PRIM_DOUBLE);
            context.register(Double.class,        Writes.DOUBLE,          Reads.DOUBLE);
            context.register(String.class,        Writes.STRING,          Reads.STRING);
            context.register(LocalDate.class,     Writes.LOCAL_DATE,      Reads.LOCAL_DATE);
            context.register(LocalTime.class,     Writes.LOCAL_TIME,      Reads.LOCAL_TIME);
            context.register(LocalDateTime.class, Writes.LOCAL_DATE_TIME, Reads.LOCAL_DATE_TIME);
            context.register(byte[].class,        Writes.BYTE_ARRAY,      Reads.BYTE_ARRAY);
            return context;
        }

        public static Builder createFromContext(Context context) {
            return new Builder(context.readers, context.writers);
        }

        private Builder(Reads.Map readers, Writes.Map writers) {
            this.readers = readers;
            this.writers = writers;
        }

        /** Tells the builder how you want to map a Java type to SQL, both going in to SQL ({@code Write}) and out of SQL ({@code Read}) */
        public <T> Builder register(Class<T> klass, Write<T> write, Read<T> read) {
            readers.put(klass, read);
            writers.put(klass, write);
            return this;
        }

        /** Tells the builder how you want to construct a Java value from a SQL value */
        public <T> Builder registerRead(Class<? super T> klass, Read<T> read) {
            readers.put(klass, read);
            return this;
        }

        /** Tells the builder how you want to construct a SQL value from a Java value */
        public <T> Builder registerWrite(Class<? extends T> klass, Write<T> write) {
            writers.put(klass, write);
            return this;
        }

        /** Convenience for registered a JavaBean for both reading and writing simultaneously */
        public <T> Builder registerBean(Class<T> klass, String... fields) {
            return register(klass, Writes.bean(klass, fields), Reads.bean(klass, fields));
        }

        public Context build() {
            return new Context(new Reads.Map(readers), new Writes.Map(writers));
        }
    }

    private final Reads.Map readers;
    private final Writes.Map writers;

    private Context(Reads.Map readers, Writes.Map writers) {
        this.readers = readers;
        this.writers = writers;
    }

    public Read.Context readContext() { return readers; }
    public Write.Context writeContext() { return writers; }
}
