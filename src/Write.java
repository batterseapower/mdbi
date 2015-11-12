import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;

interface Write<T> {
    Write<Integer> INT = new Write<Integer>() {
        @Override
        public void set(PreparedStatement s, IndexRef ix, Integer x) throws SQLException {
            s.setInt(ix.x++, x.intValue());
        }
    };
    Write<Integer> INTEGER = new Write<Integer>() {
        @Override
        public void set(PreparedStatement s, IndexRef ix, Integer x) throws SQLException {
            if (x == null) {
                s.setNull(ix.x++, Types.INTEGER);
            } else {
                s.setInt(ix.x++, x);
            }
        }
    };
    Write<String> STRING = new Write<String>() {
        @Override
        public void set(PreparedStatement s, IndexRef ix, String x) throws SQLException {
            if (x == null) {
                s.setNull(ix.x++, Types.VARCHAR);
            } else {
                s.setString(ix.x++, x);
            }
        }
    };

    class Map {
        private final HashMap<Class<?>, Write<?>> map = new HashMap<>();

        public <T> void put(Class<? extends T> klass, Write<T> write) {
            map.put(klass, write);
        }

        @SuppressWarnings("unchecked")
        public <T> Write<T> get(Class<T> klass) {
            return (Write<T>)map.get(klass);
        }
    }

    void set(PreparedStatement s, IndexRef ix, T x) throws SQLException;
}
