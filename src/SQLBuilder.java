import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

abstract class AbstractSQLBuilder {
    private final StringBuilder stringBuilder = new StringBuilder();

    public void visitSQL(SQL sql) {
        for (int i = 0; i < sql.args.length; i += 2) {
            stringBuilder.append((String)sql.args[i]);
            if (i + 1 < sql.args.length) {
                visitArg(sql.args[i+1]);
            }
        }
    }

    public void visitArg(Object arg) {
        if (arg instanceof SQL) {
            visitSQL((SQL)arg);
        } else {
            stringBuilder.append("?");
            visitObject(arg);
        }
    }

    protected abstract void visitObject(Object arg);

    protected String build() {
        return stringBuilder.toString();
    }
}

class BatchSQLBuilder extends AbstractSQLBuilder {
    private interface Action {
        void write(PreparedStatement stmt, IndexRef ref, Object arg) throws SQLException;
    }

    private final Write.Map wm;

    private Integer size;
    private final List<Collection> collections = new ArrayList<>();
    private final List<Action> actions = new ArrayList<>();

    public BatchSQLBuilder(Write.Map wm) {
        this.wm = wm;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void visitObject(Object argObject) {
        if (!(argObject instanceof Collection)) {
            throw new IllegalArgumentException("Batch updates expect Collections, but you supplied a " + (argObject == null ? "null" : argObject.getClass().toString()));
        }

        final Collection arg = (Collection) argObject;
        if (size == null) {
            size = arg.size();
        } else if (size != arg.size()) {
            throw new IllegalArgumentException("All collections supplied to batch update must be of the same size, but you had both sizes " + size + " and " + arg.size());
        }

        collections.add(arg);

        if (arg.size() > 0) {
            final Iterator it = arg.iterator();
            Object example = it.next();
            while (example == null && it.hasNext()) {
                example = it.next();
            }

            if (example == null) {
                // We know for sure that all elements of the column are null
                actions.add((stmt, ref, _null) -> stmt.setObject(ref.x++, null));
            } else {
                final BoundWrite<Object> write = (BoundWrite<Object>)wm.get(example.getClass()).bind(wm);
                actions.add(write::set);
            }
        }
    }

    public PreparedStatement build(Connection connection) throws SQLException {
        final PreparedStatement stmt = connection.prepareStatement(super.build());

        if (size != null) {
            final List<Iterator> iterators = collections.stream().map(Collection::iterator).collect(Collectors.toList());
            for (int i = 0; i < size; i++) {
                final IndexRef ref = new IndexRef();
                for (int j = 0; j < iterators.size(); j++) {
                    actions.get(j).write(stmt, ref, iterators.get(j).next());
                }
                stmt.addBatch();
            }
        }

        return stmt;
    }
}

class SQLBuilder extends AbstractSQLBuilder {
    private interface Action {
        void write(PreparedStatement stmt, IndexRef ref) throws SQLException;
    }

    private final List<Action> actions = new ArrayList<>();

    private final Write.Map wm;

    public SQLBuilder(Write.Map wm) {
        this.wm = wm;
    }

    @SuppressWarnings("unchecked")
    public void visitObject(Object arg) {
        if (arg == null) {
            actions.add((stmt, ref) -> stmt.setObject(ref.x++, null));
        } else {
            final Class<?> klass = arg.getClass();
            final BoundWrite<Object> write = (BoundWrite<Object>)wm.get(klass).bind(wm);
            actions.add((stmt, ref) -> write.set(stmt, ref, arg));
        }
    }

    public PreparedStatement build(Connection connection) throws SQLException {
        final PreparedStatement stmt = connection.prepareStatement(super.build());

        final IndexRef ref = new IndexRef();
        for (Action action : actions) {
            action.write(stmt, ref);
        }

        return stmt;
    }
}
