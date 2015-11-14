package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class Writes {
    public static final Write<Boolean> PRIM_BOOLEAN = new AbstractUnaryWrite<Boolean>() {
        @Override public String asSQL(@Nullable Boolean x) { assert x != null; return Boolean.toString(x); }

        @Override
        public void set(PreparedStatement s, int ix, @Nullable Boolean x) throws SQLException {
            assert x != null;
            s.setBoolean(ix, x);
        }
    };
    public static final Write<Boolean> BOOLEAN = new AbstractUnaryWrite<Boolean>() {
        @Override String asSQL(@Nullable Boolean x) { return x == null ? "null" : Boolean.toString(x); }

        @Override
        void set(PreparedStatement s, int ix, @Nullable Boolean x) throws SQLException {
            if (x == null) {
                s.setNull(ix, Types.BOOLEAN);
            } else {
                s.setBoolean(ix, x);
            }
        }
    };
    public static final Write<Byte> PRIM_BYTE = new AbstractUnaryWrite<Byte>() {
        @Override public String asSQL(@Nullable Byte x) { assert x != null; return Byte.toString(x); }
        @Override public void set(PreparedStatement s, int ix, @Nullable Byte x) throws SQLException { assert x != null; s.setByte(ix, x); }
    };
    public static final Write<Byte> BYTE = new AbstractUnaryWrite<Byte>() {
        @Override public String asSQL(@Nullable Byte x) { return x == null ? "null" : Byte.toString(x); }

        @Override
        public void set(PreparedStatement s, int ix, @Nullable Byte x) throws SQLException {
            if (x == null) {
                s.setNull(ix, Types.TINYINT);
            } else {
                s.setByte(ix, x);
            }
        }
    };
    public static final Write<Character> PRIM_CHAR = new AbstractUnaryWrite<Character>() {
        @Override String asSQL(@Nullable Character x) { assert x != null; return Character.toString(x); }
        @Override void set(PreparedStatement s, int ix, @Nullable Character x) throws SQLException { assert x != null; s.setString(ix, Character.toString(x)); }
    };
    public static final Write<Character> CHARACTER = new AbstractUnaryWrite<Character>() {
        @Override String asSQL(@Nullable Character x) { return x == null ? "null" : Character.toString(x); }
        @Override void set(PreparedStatement s, int ix, @Nullable Character x) throws SQLException { s.setString(ix, x == null ? null : Character.toString(x)); }
    };
    public static final Write<Short> PRIM_SHORT = new AbstractUnaryWrite<Short>() {
        @Override String asSQL(@Nullable Short x) { assert x != null; return Short.toString(x); }
        @Override void set(PreparedStatement s, int ix, @Nullable Short x) throws SQLException { assert x != null; s.setShort(ix, x); }
    };
    public static final Write<Short> SHORT = new AbstractUnaryWrite<Short>() {
        @Override String asSQL(@Nullable Short x) { return x == null ? "null" : Short.toString(x); }

        @Override
        void set(PreparedStatement s, int ix, @Nullable Short x) throws SQLException {
            if (x == null) {
                s.setNull(ix, Types.SMALLINT);
            } else {
                s.setShort(ix, x);
            }
        }
    };
    public static final Write<Integer> PRIM_INT = new AbstractUnaryWrite<Integer>() {
        @Override public String asSQL(@Nullable Integer x) { assert x != null; return Integer.toString(x.intValue()); }
        @Override public void set(PreparedStatement s, int ix, @Nullable Integer x) throws SQLException { assert x != null; s.setInt(ix, x); }
    };
    public static final Write<Integer> INTEGER = new AbstractUnaryWrite<Integer>() {
        @Override public String asSQL(@Nullable Integer x) { return x == null ? "null" : Integer.toString(x.intValue()); }

        @Override
        public void set(PreparedStatement s, int ix, @Nullable Integer x) throws SQLException {
            if (x == null) {
                s.setNull(ix, Types.INTEGER);
            } else {
                s.setInt(ix, x);
            }
        }
    };
    public static final Write<Long> PRIM_LONG = new AbstractUnaryWrite<Long>() {
        @Override String asSQL(@Nullable Long x) { assert x != null; return Long.toString(x); }
        @Override void set(PreparedStatement s, int ix, @Nullable Long x) throws SQLException { assert x != null; s.setLong(ix, x); }
    };
    public static final Write<Long> LONG = new AbstractUnaryWrite<Long>() {
        @Override String asSQL(@Nullable Long x) { return x == null ? "null" : Long.toString(x); }

        @Override
        void set(PreparedStatement s, int ix, @Nullable Long x) throws SQLException {
            if (x == null) {
                s.setNull(ix, Types.BIGINT);
            } else {
                s.setLong(ix, x);
            }
        }
    };
    public static final Write<Float> PRIM_FLOAT = new AbstractUnaryWrite<Float>() {
        @Override public String asSQL(@Nullable Float x) { assert x != null; return Float.isNaN(x) ? "null" : Float.toString(x); }

        @Override
        public void set(PreparedStatement s, int ix, @Nullable Float x) throws SQLException {
            assert x != null;
            if (Float.isNaN(x)) {
                s.setNull(ix, Types.FLOAT);
            } else {
                s.setFloat(ix, x);
            }
        }
    };
    public static final Write<Float> FLOAT = new AbstractUnaryWrite<Float>() {
        @Override public String asSQL(@Nullable Float x) { return (x == null || Float.isNaN(x)) ? "null" : Float.toString(x); }

        @Override
        public void set(PreparedStatement s, int ix, @Nullable Float x) throws SQLException {
            if (x == null || Float.isNaN(x)) {
                s.setNull(ix, Types.FLOAT);
            } else {
                s.setFloat(ix, x);
            }
        }
    };
    public static final Write<Double> PRIM_DOUBLE = new AbstractUnaryWrite<Double>() {
        @Override public String asSQL(@Nullable Double x) { assert x != null; return Double.isNaN(x) ? "null" : Double.toString(x); }

        @Override
        public void set(PreparedStatement s, int ix, @Nullable Double x) throws SQLException {
            assert x != null;
            if (Double.isNaN(x)) {
                s.setNull(ix, Types.DOUBLE);
            } else {
                s.setDouble(ix, x);
            }
        }
    };
    public static final Write<Double> DOUBLE = new AbstractUnaryWrite<Double>() {
        @Override public String asSQL(@Nullable Double x) { return (x == null || Double.isNaN(x)) ? "null" : Double.toString(x); }

        @Override
        public void set(PreparedStatement s, int ix, @Nullable Double x) throws SQLException {
            if (x == null || Double.isNaN(x)) {
                s.setNull(ix, Types.DOUBLE);
            } else {
                s.setDouble(ix, x);
            }
        }
    };
    public static final Write<String> STRING = new AbstractUnaryWrite<String>() {
        @Override public String asSQL(@Nullable String x) { return x == null ? null : "'" + x.replace("'", "''") + "'"; }
        @Override public void set(PreparedStatement s, int ix, @Nullable String x) throws SQLException { s.setString(ix, x); }
    };
    public static final Write<LocalDate> LOCAL_DATE = new AbstractUnaryWrite<LocalDate>() {
        @Override public String asSQL(@Nullable LocalDate x) { return x == null ? "null" : "'" + x.toString() + "'"; }

        @Override
        public void set(PreparedStatement s, int ix, @Nullable LocalDate x) throws SQLException {
            s.setTimestamp(ix, x == null ? null : new Timestamp(x.atTime(0, 0).atZone(Time.UTC_ZONE_ID).toInstant().toEpochMilli()), Time.UTC_CALENDAR.get());
        }
    };
    public static final Write<LocalTime> LOCAL_TIME = new AbstractUnaryWrite<LocalTime>() {
        @Override String asSQL(@Nullable LocalTime x) { return x == null ? "null" : "'" + x.toString() + "'"; }

        @Override
        void set(PreparedStatement s, int ix, @Nullable LocalTime x) throws SQLException {
            s.setTime(ix, x == null ? null : new java.sql.Time(x.atDate(LocalDate.of(1970, 1, 1)).atZone(Time.UTC_ZONE_ID).toInstant().toEpochMilli()), Time.UTC_CALENDAR.get());
        }
    };
    public static final Write<LocalDateTime> LOCAL_DATE_TIME = new AbstractUnaryWrite<LocalDateTime>() {
        @Override public String asSQL(@Nullable LocalDateTime x) { return x == null ? "null" : "'" + x.toString() + "'"; }

        @Override
        public void set(PreparedStatement s, int ix, @Nullable LocalDateTime x) throws SQLException {
            s.setTimestamp(ix, x == null ? null : new Timestamp(x.atZone(Time.UTC_ZONE_ID).toInstant().toEpochMilli()), Time.UTC_CALENDAR.get());
        }
    };
    public static final Write<byte[]> BYTE_ARRAY = new AbstractUnaryWrite<byte[]>() {
        @Override
        String asSQL(@Nullable byte[] x) {
            try {
                return x == null ? "null" : "'" + new String(x, "ASCII").replace("'", "''") + "'";
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("No ASCII encoding, WTF?", e);
            }
        }

        @Override
        void set(PreparedStatement s, int ix, @Nullable byte[] x) throws SQLException {
            s.setBytes(ix, x);
        }
    };

    public static <T> Write<T> useContext(Class<T> klass) {
        return new ContextWrite<>(klass);
    }

    public static <T> Write<T> bean(Class<T> klass, String... fields) {
        return new BeanWrite<>(klass, fields);
    }

    public static <T> Write<T> bean(Class<T> klass, Collection<String> fields, Collection<Write<?>> reads) {
        return new BeanWrite<>(klass, fields, reads);
    }

    private Writes() {}

    private static abstract class AbstractUnaryWrite<T> implements Write<T> {
        abstract String asSQL(@Nullable T x);
        abstract void set(PreparedStatement s, int ix, @Nullable T x) throws SQLException;

        @Override
        public BoundWrite<T> bind(Map ctxt) {
            return new BoundWrite<T>() {
                @Override
                public int arity() {
                    return 1;
                }

                @Override
                public void set(@Nonnull PreparedStatement s, @Nonnull IndexRef ix, T x) throws SQLException {
                    AbstractUnaryWrite.this.set(s, ix.take(), x);
                }

                @Nonnull
                @Override
                public List<String> asSQL(T x) {
                    return Collections.singletonList(AbstractUnaryWrite.this.asSQL(x));
                }
            };
        }
    }

    public static class Map {
        private final HashMap<Class<?>, Write<?>> map = new HashMap<>();

        public <T> void put(Class<? extends T> klass, Write<T> write) {
            map.put(klass, write);
        }

        @SuppressWarnings("unchecked")
        public <T> Write<T> get(Class<T> klass) {
            final Write<T> result = (Write<T>) map.get(klass);
            if (result == null) {
                throw new IllegalArgumentException("Don't know how to transfer " + klass + " objects to JDBC");
            } else {
                return result;
            }
        }
    }
}
