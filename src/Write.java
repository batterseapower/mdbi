import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

interface Write<T> {
    Write<Integer> INT = ctxt -> new BoundWrite<Integer>() {
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
