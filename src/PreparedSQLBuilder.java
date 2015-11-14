import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

abstract class AbstractSQLBuilder {
    protected final StringBuilder stringBuilder = new StringBuilder();

    public void visitSQL(SQL sql) {
        boolean nextMayBeParam = false;
        for (int i = 0; i < sql.args.length; i++) {
            final Object arg = sql.args[i];
            if (arg instanceof SQL) {
                visitSQL((SQL)arg);
                nextMayBeParam = true;
            } else if (arg instanceof In) {
                final In in = (In)arg;
                // Exploit the fact that 'null not in (null)' to avoid generating nullary IN clauses:
                // systems like SQL Server can't parse them
                stringBuilder.append("(null");
                for (int j = 0; j < in.args.length; j++) {
                    stringBuilder.append(',');
                    if (in.args[j] instanceof SQL) {
                        visitSQL((SQL)in.args[j]);
                    } else {
                        visitObject(in.args[j]);
                    }
                }
                stringBuilder.append(')');
            } else if (!nextMayBeParam && (arg instanceof String)) {
                stringBuilder.append((String)arg);
                nextMayBeParam = true;
            } else {
                visitObject(arg);
                nextMayBeParam = false;
            }
        }
    }

    protected abstract void visitObject(Object arg);

    protected String build() {
        return stringBuilder.toString();
    }
}

abstract class AbstractPreparedSQLBuilder extends AbstractSQLBuilder {
    @Override
    protected void visitObject(Object arg) {
        final int arity = visitPreparedObject(arg);
        for (int i = 0; i < arity; i++) {
            if (i != 0) stringBuilder.append(',');
            stringBuilder.append("?");
        }
    }

    protected abstract int visitPreparedObject(Object arg);

    protected PreparedStatement build(Connection connection) throws SQLException {
        return connection.prepareStatement(super.build());
    }
}

abstract class AbstractUnpreparedSQLBuilder extends AbstractSQLBuilder {
    @Override
    protected void visitObject(Object arg) {
        final List<String> xs = visitUnpreparedObject(arg);
        for (int i = 0; i < xs.size(); i++) {
            if (i != 0) stringBuilder.append(',');
            stringBuilder.append(xs.get(i));
        }
    }

    protected abstract List<String> visitUnpreparedObject(Object arg);
}

class BatchBuilder {
    private final Write.Map wm;

    private final List<Collection> collections = new ArrayList<>();
    private Integer size;

    public BatchBuilder(Write.Map wm) {
        this.wm = wm;
    }

    public BoundWrite<Object> visit(Object argObject) {
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

        final Class<?> klass;
        if (arg.size() > 0) {
            final Iterator it = arg.iterator();
            Object example = it.next();
            while (example == null && it.hasNext()) {
                example = it.next();
            }

            klass = example == null ? null : example.getClass();
        } else {
            klass = null;
        }

        // FIXME: have bind() take the object too, and then find one binder per elt here?
        // Would mean we could have a Collection Write class that Just Works, rather than the "In" special case... (except for the nullary wrinkle..)
        if (klass == null) {
            // We know for sure that all elements of the column are null
            // TODO: this is a bit dodgy! We should at least provide some way to indicate the type explicitly if you want to avoid ever hitting this case.
            return new BoundWriteNull();
        } else {
            return (BoundWrite<Object>)wm.get(klass).bind(wm);
        }
    }

    public Map.Entry<Integer, List<Collection>> build() {
        return new AbstractMap.SimpleImmutableEntry<Integer, List<Collection>>(size == null ? 0 : size, collections);
    }
}

class BoundWriteNull implements BoundWrite<Object> {
    @Override
    public int arity() {
        return 1;
    }

    @Override
    public void set(PreparedStatement s, IndexRef ix, Object x) throws SQLException {
        s.setObject(ix.x++, null);
    }

    @Override
    public List<String> asSQL(Object x) {
        return Collections.singletonList("null");
    }
}

class BatchUnpreparedSQLBuilder extends AbstractUnpreparedSQLBuilder {
    private final BatchBuilder batch;
    private final List<Map.Entry<BoundWrite<Object>, List<String>>> boundWrites = new ArrayList<>();

    public BatchUnpreparedSQLBuilder(Write.Map wm) {
        batch = new BatchBuilder(wm);
    }

    @Override
    protected List<String> visitUnpreparedObject(Object arg) {
        final BoundWrite<Object> boundWrite = batch.visit(arg);

        final List<String> result = new ArrayList<>();
        for (int i = 0; i < boundWrite.arity(); i++) {
            result.add(UUID.randomUUID().toString());
        }

        boundWrites.add(new AbstractMap.SimpleImmutableEntry<BoundWrite<Object>, List<String>>(boundWrite, result));
        return result;
    }

    // TODO: should be called "build"
    public Map.Entry<Integer, Iterator<String>> buildIterator() {
        final String sql = super.build();
        final Map.Entry<Integer, List<Collection>> batchBuilt = batch.build();
        final int size = batchBuilt.getKey();

        final List<Iterator> iterators = batchBuilt.getValue().stream().map(Collection::iterator).collect(Collectors.toList());
        return new AbstractMap.SimpleImmutableEntry<Integer, Iterator<String>>(
                size,
                new Iterator<String>() {
                    private int i = 0;

                    @Override
                    public boolean hasNext() {
                        return i < size;
                    }

                    @Override
                    public String next() {
                        String result = sql;
                        for (int j = 0; j < boundWrites.size(); j++) {
                            final Map.Entry<BoundWrite<Object>, List<String>> boundWrite = boundWrites.get(j);
                            final List<String> replacements = boundWrite.getKey().asSQL(iterators.get(j).next());
                            for (int k = 0; k < boundWrite.getValue().size(); k++) {
                                result = result.replace(boundWrite.getValue().get(k), replacements.get(k));
                            }
                        }

                        i++;
                        return result;
                    }
                });
    }
}

class BatchPreparedSQLBuilder extends AbstractPreparedSQLBuilder {
    private interface Action {
        void write(PreparedStatement stmt, IndexRef ref, Object arg) throws SQLException;
    }

    private final BatchBuilder batch;
    private final List<Action> actions = new ArrayList<>();

    public BatchPreparedSQLBuilder(Write.Map wm) {
        this.batch = new BatchBuilder(wm);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected int visitPreparedObject(Object argObject) {
        final BoundWrite<Object> write = batch.visit(argObject);
        actions.add(write::set);
        return write.arity();
    }

    public PreparedStatement build(Connection connection) throws SQLException {
        final PreparedStatement stmt = super.build(connection);
        final Map.Entry<Integer, List<Collection>> batchBuilt = batch.build();

        final List<Iterator> iterators = batchBuilt.getValue().stream().map(Collection::iterator).collect(Collectors.toList());
        for (int i = 0; i < batchBuilt.getKey(); i++) {
            final IndexRef ref = new IndexRef();
            for (int j = 0; j < iterators.size(); j++) {
                actions.get(j).write(stmt, ref, iterators.get(j).next());
            }
            stmt.addBatch();
        }

        return stmt;
    }
}

class UnpreparedSQLBuilder extends AbstractUnpreparedSQLBuilder {
    private final Write.Map wm;

    public UnpreparedSQLBuilder(Write.Map wm) {
        this.wm = wm;
    }

    public List<String> visitUnpreparedObject(Object arg) {
        final BoundWrite<Object> write = PreparedSQLBuilder.getObjectBoundWrite(wm, arg);
        return write.asSQL(arg);
    }
}

class PreparedSQLBuilder extends AbstractPreparedSQLBuilder {
    private interface Action {
        void write(PreparedStatement stmt, IndexRef ref) throws SQLException;
    }

    private final List<Action> actions = new ArrayList<>();

    private final Write.Map wm;

    public PreparedSQLBuilder(Write.Map wm) {
        this.wm = wm;
    }

    public int visitPreparedObject(Object arg) {
        final BoundWrite<Object> write = getObjectBoundWrite(wm, arg);
        actions.add((stmt, ref) -> write.set(stmt, ref, arg));
        return write.arity();
    }

    @SuppressWarnings("unchecked")
    static BoundWrite<Object> getObjectBoundWrite(Write.Map wm, Object arg) {
        BoundWrite<Object> write;
        if (arg == null) {
            // TODO: this is a bit dodgy! We should at least provide some way to indicate the type explicitly if you want to avoid ever hitting this case.
            write = new BoundWriteNull();
        } else {
            final Class<?> klass = arg.getClass();
            write = (BoundWrite<Object>)wm.get(klass).bind(wm);
        }
        return write;
    }

    public PreparedStatement build(Connection connection) throws SQLException {
        final PreparedStatement stmt = super.build(connection);

        final IndexRef ref = new IndexRef();
        for (Action action : actions) {
            action.write(stmt, ref);
        }

        return stmt;
    }
}
