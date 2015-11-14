import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public interface Read<T> {
    Read<Integer> PRIM_INT = new Read<Integer>() {
        @Override
        public Class<Integer> getElementClass() {
            return int.class;
        }

        @Override
        public BoundRead<Integer> bind(Read.Map ctxt) {
            return (rs, ix) -> {
                final int result = rs.getInt(ix.x++);
                if (rs.wasNull()) throw new IllegalArgumentException("Found null in result");
                return result;
            };
        }
    };
    Read<Integer> INTEGER = new Read<Integer>() {
        @Override
        public Class<Integer> getElementClass() {
            return Integer.class;
        }

        @Override
        public BoundRead<Integer> bind(Read.Map ctxt) {
            return (rs, ix) -> {
                final int result = rs.getInt(ix.x++);
                if (rs.wasNull()) return null;
                return result;
            };
        }
    };
    Read<String> STRING = new Read<String>() {
        @Override
        public Class<String> getElementClass() {
            return String.class;
        }

        @Override
        public BoundRead<String> bind(Read.Map ctxt) {
            return (rs, ix) -> rs.getString(ix.x++);
        }
    };
    Read<LocalDate> LOCAL_DATE = new Read<LocalDate>() {
        @Override
        public Class<LocalDate> getElementClass() {
            return LocalDate.class;
        }

        @Override
        public BoundRead<LocalDate> bind(Map ctxt) {
            return (rs, ix) -> Instant.ofEpochMilli(rs.getTimestamp(ix.x++, Time.UTC_CALENDAR.get()).getTime()).atZone(Time.UTC_ZONE_ID).toLocalDateTime().toLocalDate();
        }
    };
    Read<LocalDateTime> LOCAL_DATE_TIME = new Read<LocalDateTime>() {
        @Override
        public Class<LocalDateTime> getElementClass() {
            return LocalDateTime.class;
        }

        @Override
        public BoundRead<LocalDateTime> bind(Map ctxt) {
            return (rs, ix) -> Instant.ofEpochMilli(rs.getTimestamp(ix.x++, Time.UTC_CALENDAR.get()).getTime()).atZone(Time.UTC_ZONE_ID).toLocalDateTime();
        }
    };
    Read<Double> PRIM_DOUBLE = new Read<Double>() {
        @Override
        public Class<Double> getElementClass() {
            return double.class;
        }

        @Override
        public BoundRead<Double> bind(Map ctxt) {
            return (rs, ix) -> {
                final double result = rs.getDouble(ix.x++);
                if (rs.wasNull()) return Double.NaN;
                return result;
            };
        }
    };
    Read<Double> DOUBLE = new Read<Double>() {
        @Override
        public Class<Double> getElementClass() {
            return Double.class;
        }

        @Override
        public BoundRead<Double> bind(Map ctxt) {
            return (rs, ix) -> {
                final double result = rs.getDouble(ix.x++);
                if (rs.wasNull()) return null;
                return result;
            };
        }
    };

    class Map {
        private final HashMap<Class<?>, Read<?>> map = new HashMap<>();

        public <T> void put(Class<? super T> klass, Read<T> write) {
            map.put(klass, write);
        }

        @SuppressWarnings("unchecked")
        public <T> Read<T> get(Class<T> klass) {
            final Read<T> result = (Read<T>) map.get(klass);
            if (result == null) {
                throw new IllegalArgumentException("Don't know how to transfer " + klass + " objects from JDBC");
            } else {
                return result;
            }
        }
    }

    Class<T> getElementClass();

    BoundRead<T> bind(Read.Map ctxt);
}
