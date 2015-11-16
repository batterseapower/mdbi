package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.function.Function;

public class Reads {
    public static final Read<Boolean> PRIM_BOOLEAN = new AbstractUnaryRead<Boolean>(boolean.class) {
        @Override
        public Boolean get(ResultSet rs, int ix) throws SQLException {
            boolean result = rs.getBoolean(ix);
            if (rs.wasNull()) throw new IllegalArgumentException("Found null in result");
            return result;
        }
    };
    public static final Read<Boolean> BOOLEAN = new AbstractUnaryRead<Boolean>(Boolean.class) {
        @Override
        public Boolean get(ResultSet rs, int ix) throws SQLException {
            boolean result = rs.getBoolean(ix);
            if (rs.wasNull()) return null;
            return result;
        }
    };
    public static final Read<Byte> PRIM_BYTE = new AbstractUnaryRead<Byte>(byte.class) {
        @Override
        protected Byte get(ResultSet rs, int ix) throws SQLException {
            byte result = rs.getByte(ix);
            if (rs.wasNull()) throw new IllegalArgumentException("Found null in result");
            return result;
        }
    };
    public static final Read<Byte> BYTE = new AbstractUnaryRead<Byte>(Byte.class) {
        @Override
        public Byte get(ResultSet rs, int ix) throws SQLException {
            byte result = rs.getByte(ix);
            if (rs.wasNull()) return null;
            return result;
        }
    };
    public static final Read<Character> PRIM_CHAR = new AbstractUnaryRead<Character>(char.class) {
        @Override
        protected Character get(ResultSet rs, int ix) throws SQLException {
            String result = rs.getString(ix);
            if (rs.wasNull()) throw new IllegalArgumentException("Found null in result");
            if (result.length() != 1) throw new IllegalArgumentException("Found string " + result + " but was expecting single char");
            return result.charAt(0);
        }
    };
    public static final Read<Character> CHARACTER = new AbstractUnaryRead<Character>(Character.class) {
        @Override
        public Character get(ResultSet rs, int ix) throws SQLException {
            String result = rs.getString(ix);
            if (rs.wasNull()) return null;
            if (result.length() != 1) throw new IllegalArgumentException("Found string " + result + " but was expecting single char");
            return result.charAt(0);
        }
    };
    public static final Read<Short> PRIM_SHORT = new AbstractUnaryRead<Short>(short.class) {
        @Override
        protected Short get(ResultSet rs, int ix) throws SQLException {
            short result = rs.getShort(ix);
            if (rs.wasNull()) throw new IllegalArgumentException("Found null in result");
            return result;
        }
    };
    public static final Read<Short> SHORT = new AbstractUnaryRead<Short>(Short.class) {
        @Override
        public Short get(ResultSet rs, int ix) throws SQLException {
            short result = rs.getShort(ix);
            if (rs.wasNull()) return null;
            return result;
        }
    };
    public static final Read<Integer> PRIM_INT = new AbstractUnaryRead<Integer>(int.class) {
        @Override
        public Integer get(ResultSet rs, int ix) throws SQLException {
            final int result = rs.getInt(ix);
            if (rs.wasNull()) throw new IllegalArgumentException("Found null in result");
            return result;
        }
    };
    public static final Read<Integer> INTEGER = new AbstractUnaryRead<Integer>(Integer.class) {
        @Override
        public Integer get(ResultSet rs, int ix) throws SQLException {
            final int result = rs.getInt(ix);
            if (rs.wasNull()) return null;
            return result;
        }
    };
    public static final Read<Long> PRIM_LONG = new AbstractUnaryRead<Long>(long.class) {
        @Override
        protected Long get(ResultSet rs, int ix) throws SQLException {
            long result = rs.getLong(ix);
            if (rs.wasNull()) throw new IllegalArgumentException("Found null in result");
            return result;
        }
    };
    public static final Read<Long> LONG = new AbstractUnaryRead<Long>(Long.class) {
        @Override
        public Long get(ResultSet rs, int ix) throws SQLException {
            long result = rs.getLong(ix);
            if (rs.wasNull()) return null;
            return result;
        }
    };
    public static final Read<Float> PRIM_FLOAT = new AbstractUnaryRead<Float>(float.class) {
        @Override
        public Float get(ResultSet rs, int ix) throws SQLException {
            final float result = rs.getFloat(ix);
            if (rs.wasNull()) return Float.NaN;
            return result;
        }
    };
    public static final Read<Float> FLOAT = new AbstractUnaryRead<Float>(Float.class) {
        @Override
        public Float get(ResultSet rs, int ix) throws SQLException {
            final float result = rs.getFloat(ix);
            if (rs.wasNull()) return null;
            return result;
        }
    };
    public static final Read<Double> PRIM_DOUBLE = new AbstractUnaryRead<Double>(double.class) {
        @Override
        public Double get(ResultSet rs, int ix) throws SQLException {
            final double result = rs.getDouble(ix);
            if (rs.wasNull()) return Double.NaN;
            return result;
        }
    };
    public static final Read<Double> DOUBLE = new AbstractUnaryRead<Double>(Double.class) {
        @Override
        public Double get(ResultSet rs, int ix) throws SQLException {
            final double result = rs.getDouble(ix);
            if (rs.wasNull()) return null;
            return result;
        }
    };
    public static final Read<String> STRING = new AbstractUnaryRead<String>(String.class) {
        @Override public String get(ResultSet rs, int ix) throws SQLException { return rs.getString(ix); }
    };
    public static final Read<LocalDate> LOCAL_DATE = new AbstractUnaryRead<LocalDate>(LocalDate.class) {
        @Override
        public LocalDate get(ResultSet rs, int ix) throws SQLException {
            final Timestamp ts = rs.getTimestamp(ix, Time.UTC_CALENDAR.get());
            return ts == null ? null : Instant.ofEpochMilli(ts.getTime()).atZone(Time.UTC_ZONE_ID).toLocalDate();
        }
    };
    public static final Read<LocalTime> LOCAL_TIME = new AbstractUnaryRead<LocalTime>(LocalTime.class) {
        @Override
        public LocalTime get(ResultSet rs, int ix) throws SQLException {
            final Timestamp ts = rs.getTimestamp(ix, Time.UTC_CALENDAR.get());
            return ts == null ? null : Instant.ofEpochMilli(ts.getTime()).atZone(Time.UTC_ZONE_ID).toLocalTime();
        }
    };
    public static final Read<LocalDateTime> LOCAL_DATE_TIME = new AbstractUnaryRead<LocalDateTime>(LocalDateTime.class) {
        @Override
        public LocalDateTime get(ResultSet rs, int ix) throws SQLException {
            final Timestamp ts = rs.getTimestamp(ix, Time.UTC_CALENDAR.get());
            return ts == null ? null : Instant.ofEpochMilli(ts.getTime()).atZone(Time.UTC_ZONE_ID).toLocalDateTime();
        }
    };
    public static final Read<byte[]> BYTE_ARRAY = new AbstractUnaryRead<byte[]>(byte[].class) {
        @Override protected byte[] get(ResultSet rs, int ix) throws SQLException { return rs.getBytes(ix); }
    };

    public static <T> Read<T> useContext(Class<T> klass) {
        return new ContextRead<>(klass);
    }

    public static <T> Read<T> tuple(Class<T> klass) {
        return new TupleRead<T>(klass);
    }

    public static <T> Read<T> tuple(Class<T> klass, Collection<Read<?>> reads) {
        return new TupleRead<T>(klass, reads);
    }

    public static <T, U> Read<U> map(Class<U> klass, Read<T> read, Function<T, U> f) {
        return new Read<U>() {
            @Override
            public Class<U> getElementClass() {
                return klass;
            }

            @Override
            public BoundRead<? extends U> bind(Map ctxt) {
                final BoundRead<? extends T> boundRead = read.bind(ctxt);
                return (rs, ix) -> f.apply(boundRead.get(rs, ix));
            }
        };
    }

    private Reads() {}

    public static class Map {
        private final HashMap<Class<?>, Read<?>> map = new HashMap<>();

        public <T> void put(Class<? super T> klass, Read<T> write) {
            map.put(klass, write);
        }

        @SuppressWarnings("unchecked")
        public <T> Read<? extends T> get(Class<T> klass) {
            final Read<? extends T> result = (Read<? extends T>) map.get(klass);
            if (result == null) {
                throw new IllegalArgumentException("Don't know how to transfer " + klass + " objects from JDBC");
            } else {
                return result;
            }
        }
    }

    private abstract static class AbstractUnaryRead<T> implements Read<T> {
        private final Class<T> klass;

        public AbstractUnaryRead(Class<T> klass) {
            this.klass = klass;
        }

        @Override
        public Class<T> getElementClass() {
            return klass;
        }

        @Override
        public BoundRead<T> bind(Reads.Map ctxt) {
            return new BoundRead<T>() {
                @Override
                public T get(@Nonnull ResultSet rs, @Nonnull IndexRef ix) throws SQLException {
                    return AbstractUnaryRead.this.get(rs, ix.take());
                }
            };
        }

        protected abstract T get(ResultSet rs, int ix) throws SQLException;
    }

    public static <T> Read<T> bean(Class<T> klass, String... fields) {
        return new BeanRead<>(klass, fields);
    }

    public static <T> Read<T> bean(Class<T> klass, Collection<String> fields, Collection<Read<?>> reads) {
        return new BeanRead<>(klass, fields, reads);
    }
}
