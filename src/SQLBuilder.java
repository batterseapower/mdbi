import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class SQLBuilder {
    private interface Action {
        public void write(PreparedStatement stmt, IndexRef ref) throws SQLException;
    }

    private final StringBuilder stringBuilder = new StringBuilder();
    private final List<Action> actions = new ArrayList<>();

    private final Write.Map wm;

    public SQLBuilder(Write.Map wm) {
        this.wm = wm;
    }

    public void visitSQL(SQL sql) {
        for (int i = 0; i < sql.args.length; i += 2) {
            stringBuilder.append((String)sql.args[i]);
            if (i + 1 < sql.args.length) {
                visitArg(sql.args[i+1]);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void visitArg(Object arg) {
        if (arg instanceof SQL) {
            visitSQL((SQL)arg);
        } else {
            stringBuilder.append("?");
            if (arg == null) {
                actions.add((stmt, ref) -> stmt.setObject(ref.x++, null));
            } else {
                final Class<?> klass = arg.getClass();
                final Write<?> write = wm.get(klass);
                if (write == null) {
                    throw new IllegalArgumentException("Don't know how to transfer " + klass + " objects to JDBC");
                } else {
                    actions.add((stmt, ref) -> ((Write<Object>)write).set(stmt, ref, arg));
                }
            }
        }
    }

    public PreparedStatement build(Connection connection) throws SQLException {
        final PreparedStatement stmt = connection.prepareStatement(stringBuilder.toString());

        final IndexRef ref = new IndexRef();
        for (Action action : actions) {
            action.write(stmt, ref);
        }

        return stmt;
    }

}
