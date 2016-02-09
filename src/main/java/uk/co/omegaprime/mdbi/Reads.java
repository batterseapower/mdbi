package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Useful methods for constructing instances of the {@link Read} interface. */
public class Reads {
    public static final Read<Boolean> PRIM_BOOLEAN = new AbstractUnaryRead<Boolean>(boolean.class) {
        @Override
        public Boolean get(ResultSet rs, int ix) throws SQLException {
            boolean result = rs.getBoolean(ix);
            if (rs.wasNull()) throw new NullPointerException("Found null in result");
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
            if (rs.wasNull()) throw new NullPointerException("Found null in result");
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
            if (rs.wasNull()) throw new NullPointerException("Found null in result");
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
            if (rs.wasNull()) throw new NullPointerException("Found null in result");
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
            if (rs.wasNull()) throw new NullPointerException("Found null in result");
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
            if (rs.wasNull()) throw new NullPointerException("Found null in result");
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
    public static final Read<BigDecimal> BIG_DECIMAL = new AbstractUnaryRead<BigDecimal>(BigDecimal.class) {
        @Override protected BigDecimal get(ResultSet rs, int ix) throws SQLException { return rs.getBigDecimal(ix); }
    };

    /** A {@code Read} instance that simply defers to the {@link Context} to decide how to construct an instance of the given class. */
    public static <T> Read<T> useContext(Class<T> klass) {
        return new ContextRead<>(klass);
    }

    /** Constructs a type that has only one public constructor. Constructor arguments are recursively constructed using the {@link Context}. */
    public static <T> Read<T> tuple(Class<T> klass) {
        return new TupleRead<T>(klass);
    }

    /** Constructs a type that has only one public constructor. Constructor arguments are constructed using the context-default {@code Read} instance for the supplied classes. */
    public static <T> Read<T> tupleWithFieldClasses(Class<T> klass, Collection<Class<?>> klasses) {
        return tuple(klass, klasses.stream().map(argklass -> new ContextRead<>(argklass)).collect(Collectors.toList()));
    }

    /** Constructs a type that has only one public constructor. Constructor arguments are constructed using the supplied {@code Read} instances. */
    public static <T> Read<T> tuple(Class<T> klass, Collection<Read<?>> reads) {
        return new TupleRead<T>(klass, reads);
    }

    /** Mapping treating {@code Read} as a functor. */
    public static <T, U> Read<U> map(Class<U> klass, Class<T> readKlass, Function<T, U> f) {
        return map(klass, new ContextRead<>(readKlass), f);
    }

    /** Mapping treating {@code Read} as a functor. */
    public static <T, U> Read<U> map(Class<U> klass, Read<T> read, Function<T, U> f) {
        return new Read<U>() {
            @Override
            public Class<U> getElementClass() {
                return klass;
            }

            @Override
            public BoundRead<? extends U> bind(Context ctxt) {
                final BoundRead<? extends T> boundRead = read.bind(ctxt);
                return new BoundRead<U>() {
                    @Override
                    public int arity() {
                        return boundRead.arity();
                    }

                    @Override
                    public U get(@Nonnull ResultSet rs, @Nonnull IndexRef ix) throws SQLException {
                        return f.apply(boundRead.get(rs, ix));
                    }
                };
            }
        };
    }

    private Reads() {}

    static class Map implements Read.Context {
        private final HashMap<Class<?>, Read<?>> map = new HashMap<>();

        public Map() {}

        public Map(Map that) {
            map.putAll(that.map);
        }

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
        public BoundRead<T> bind(Context ctxt) {
            return new BoundRead<T>() {
                @Override
                public int arity() {
                    return 1;
                }

                @Override
                public T get(@Nonnull ResultSet rs, @Nonnull IndexRef ix) throws SQLException {
                    return AbstractUnaryRead.this.get(rs, ix.take());
                }
            };
        }

        protected abstract T get(ResultSet rs, int ix) throws SQLException;
    }

    /**
     * Constructs a bean using reflection.
     * <p>
     * The named bean properties are extracted from the SQL result in the order given. The value of the bean
     * property is constructed using the default {@code Read} associated for that type in the {@link Context}.
     */
    public static <T> Read<T> bean(Class<T> klass, String... fields) {
        return new BeanRead<>(klass, fields);
    }

    /** As {@link #bean(Class, String...)}, but allows you to explicitly specify the types of the fields. */
    public static <T> Read<T> beanWithFieldClasses(Class<T> klass, Collection<String> fields, Collection<Class<?>> klasses) {
        return new BeanRead<>(klass, fields, klasses.stream().map(argklass -> new ContextRead<>(argklass)).collect(Collectors.toList()));
    }

    /** As {@link #bean(Class, String...)}, but allows you to customize how the property values are constructed. */
    public static <T> Read<T> bean(Class<T> klass, Collection<String> fields, Collection<Read<?>> reads) {
        return new BeanRead<>(klass, fields, reads);
    }

    /** Reads the given classes one after another, aggregating the results from the row into a {@code List} */
    public static <T> Read<List<T>> listWithClasses(Collection<Class<? extends T>> klasses) {
        return list(klasses.stream().map(klass -> new ContextRead<>(klass)).collect(Collectors.toList()));
    }

    /** Reads the given elements one after another, aggregating the results from the row into a {@code List} */
    public static <T> Read<List<T>> list(Collection<Read<? extends T>> reads) {
        return new Read<List<T>>() {
            @SuppressWarnings("unchecked")
            @Override
            public Class<List<T>> getElementClass() {
                return (Class<List<T>>)(Class)List.class;
            }

            @Override
            public BoundRead<? extends List<T>> bind(Context ctxt) {
                final List<BoundRead<? extends T>> bounds = reads.stream().map(read -> read.bind(ctxt)).collect(Collectors.toList());

                return new BoundRead<List<T>>() {
                    @Override
                    public int arity() {
                        return bounds.stream().mapToInt(BoundRead::arity).sum();
                    }

                    @Override
                    public List<T> get(@Nonnull ResultSet rs, @Nonnull IndexRef ix) throws SQLException {
                        final List<T> result = new ArrayList<>();
                        for (BoundRead<? extends T> bound : bounds) {
                            result.add(bound.get(rs, ix));
                        }
                        return result;
                    }
                };
            }
        };
    }

    /** As {@link #labelledMap(Collection)}, but using the {@code Read} instance associated with the class in the {@link Context}. */
    public static <T> Read<java.util.Map<String, T>> labelledMapWithClasses(Collection<Class<? extends T>> klasses) {
        return labelledMap(klasses.stream().map(klass -> new ContextRead<>(klass)).collect(Collectors.toList()));
    }

    /**
     * Reads the given classes one after another, aggregating the results from the row into a {@code Map} keyed by the column name.
     * <p>
     * If a {@code Read} instance spans more than one column, the name chosen will be that of the first column.
     * <p>
     * If more than one column shares the same name, {@code IllegalArgumentException} will be thrown.
     */
    public static <T> Read<java.util.Map<String, T>> labelledMap(Collection<Read<? extends T>> reads) {
        return new Read<java.util.Map<String, T>>() {
            @SuppressWarnings("unchecked")
            @Override
            public Class<java.util.Map<String, T>> getElementClass() {
                return (Class<java.util.Map<String, T>>)(Class)java.util.Map.class;
            }

            @Override
            public BoundRead<? extends java.util.Map<String, T>> bind(Context ctxt) {
                final List<BoundRead<? extends T>> bounds = reads.stream().map(read -> read.bind(ctxt)).collect(Collectors.toList());

                return new BoundRead<java.util.Map<String, T>>() {
                    @Override
                    public int arity() {
                        return bounds.stream().mapToInt(BoundRead::arity).sum();
                    }

                    @Override
                    public java.util.Map<String, T> get(@Nonnull ResultSet rs, @Nonnull IndexRef ix) throws SQLException {
                        final java.util.Map<String, T> result = new LinkedHashMap<>();
                        for (BoundRead<? extends T> bound : bounds) {
                            // TODO: refactor so that we can do this lookup only once per query..
                            final String columnName = rs.getMetaData().getColumnName(ix.peek());
                            if (result.containsKey(columnName)) {
                                throw new IllegalArgumentException("Column " + columnName + " occurs twice in the result");
                            }

                            result.put(columnName, bound.get(rs, ix));
                        }
                        return result;
                    }
                };
            }
        };
    }
}
