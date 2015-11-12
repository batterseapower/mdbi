import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;

interface Write<T> {
    Write<Integer> INT = ctxt -> (s, ix, x) -> s.setInt(ix.x++, x.intValue());
    Write<Integer> INTEGER = ctxt -> (s, ix, x) -> {
        if (x == null) {
            s.setNull(ix.x++, Types.INTEGER);
        } else {
            s.setInt(ix.x++, x);
        }
    };
    Write<String> STRING = ctxt -> (s, ix, x) -> {
        if (x == null) {
            s.setNull(ix.x++, Types.VARCHAR);
        } else {
            s.setString(ix.x++, x);
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
