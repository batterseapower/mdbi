import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public interface Read<T> {
    Read<Integer> INT = new Read<Integer>() {
        @Override
        public Class<Integer> getElementClass() {
            return int.class;
        }

        @Override
        public Integer get(ResultSet rs, IndexRef ix) throws SQLException {
            final int result = rs.getInt(ix.x++);
            if (rs.wasNull()) throw new IllegalArgumentException("Found null in result");
            return result;
        }
    };
    Read<Integer> INTEGER = new Read<Integer>() {
        @Override
        public Class<Integer> getElementClass() {
            return Integer.class;
        }

        @Override
        public Integer get(ResultSet rs, IndexRef ix) throws SQLException {
            final int result = rs.getInt(ix.x++);
            if (rs.wasNull()) return null;
            return result;
        }
    };
    Read<String> STRING = new Read<String>() {
        @Override
        public Class<String> getElementClass() {
            return String.class;
        }

        @Override
        public String get(ResultSet rs, IndexRef ix) throws SQLException {
            return rs.getString(ix.x++);
        }
    };

    class Map {
        private final HashMap<Class<?>, Read<?>> map = new HashMap<>();

        public <T> void put(Class<? super T> klass, Read<T> write) {
            map.put(klass, write);
        }

        @SuppressWarnings("unchecked")
        public <T> Read<T> get(Class<T> klass) {
            return (Read<T>)map.get(klass);
        }
    }

    Class<T> getElementClass();

    T get(ResultSet rs, IndexRef ix) throws SQLException;
}
