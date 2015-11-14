import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

interface Write<T> {
    Write<Integer> PRIM_INT = ctxt -> new BoundWrite<Integer>() {
        @Override
        public int arity() {
            return 1;
        }

        @Override
        public void set(PreparedStatement s, IndexRef ix, Integer x) throws SQLException {
            s.setInt(ix.x++, x.intValue());
        }

        @Override
        public List<String> asSQL(Integer x) {
            return Collections.singletonList(Integer.toString(x.intValue()));
        }
    };
    Write<Integer> INTEGER = ctxt -> new BoundWrite<Integer>() {
        @Override
        public int arity() {
            return 1;
        }

        @Override
        public void set(PreparedStatement s, IndexRef ix, Integer x) throws SQLException {
            if (x == null) {
                s.setNull(ix.x++, Types.INTEGER);
            } else {
                s.setInt(ix.x++, x);
            }
        }

        @Override
        public List<String> asSQL(Integer x) {
            return Collections.singletonList(x == null ? "null" : Integer.toString(x.intValue()));
        }
    };
    Write<String> STRING = ctxt -> new BoundWrite<String>() {
        @Override
        public int arity() {
            return 1;
        }

        @Override
        public void set(PreparedStatement s, IndexRef ix, String x) throws SQLException {
            if (x == null) {
                s.setNull(ix.x++, Types.VARCHAR);
            } else {
                s.setString(ix.x++, x);
            }
        }

        @Override
        public List<String> asSQL(String x) {
            return Collections.singletonList("'" + x.replace("'", "''") + "'");
        }
    };
    Write<LocalDate> LOCAL_DATE = ctxt -> new BoundWrite<LocalDate>() {
        @Override
        public int arity() {
            return 1;
        }

        @Override
        public void set(PreparedStatement s, IndexRef ix, LocalDate x) throws SQLException {
            s.setTimestamp(ix.x++, new Timestamp(x.atTime(0, 0).atZone(Time.UTC_ZONE_ID).toInstant().toEpochMilli()), Time.UTC_CALENDAR.get());
        }

        @Override
        public List<String> asSQL(LocalDate x) {
            return Collections.singletonList("'" + x.toString() + "'");
        }
    };
    Write<LocalDateTime> LOCAL_DATE_TIME = ctxt -> new BoundWrite<LocalDateTime>() {
        @Override
        public int arity() {
            return 1;
        }

        @Override
        public void set(PreparedStatement s, IndexRef ix, LocalDateTime x) throws SQLException {
            s.setTimestamp(ix.x++, new Timestamp(x.atZone(Time.UTC_ZONE_ID).toInstant().toEpochMilli()), Time.UTC_CALENDAR.get());
        }

        @Override
        public List<String> asSQL(LocalDateTime x) {
            return Collections.singletonList("'" + x.toString() + "'");
        }
    };
    Write<Double> PRIM_DOUBLE = ctxt -> new BoundWrite<Double>() {
        @Override
        public int arity() {
            return 1;
        }

        @Override
        public void set(PreparedStatement s, IndexRef ix, Double x) throws SQLException {
            if (Double.isNaN(x)) {
                s.setNull(ix.x++, Types.DOUBLE);
            } else {
                s.setDouble(ix.x++, x);
            }
        }

        @Override
        public List<String> asSQL(Double x) {
            return Collections.singletonList(Double.isNaN(x) ? "null" : Double.toString(x));
        }
    };
    Write<Double> DOUBLE = ctxt -> new BoundWrite<Double>() {
        @Override
        public int arity() {
            return 1;
        }

        @Override
        public void set(PreparedStatement s, IndexRef ix, Double x) throws SQLException {
            if (x == null || Double.isNaN(x)) {
                s.setNull(ix.x++, Types.DOUBLE);
            } else {
                s.setDouble(ix.x++, x);
            }
        }

        @Override
        public List<String> asSQL(Double x) {
            return Collections.singletonList((x == null || Double.isNaN(x)) ? "null" : Double.toString(x));
        }
    };

    class Map {
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

    BoundWrite<T> bind(Write.Map ctxt);
}
