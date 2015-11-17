package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

class SQLBuilder {
    private final Consumer<Object> visitHole;
    private final StringBuilder stringBuilder = new StringBuilder();

    public SQLBuilder(Consumer<Object> visitHole) {
        this.visitHole = visitHole;
    }

    public void visitSQL(SQL sql) {
        for (Object arg : sql.args) {
            if (arg instanceof String) {
                visitSQLLiteral((String) arg);
            } else {
                visitHole.accept(arg);
            }
        }
    }

    public void visitSQLLiteral(String sql) {
        stringBuilder.append(sql);
    }

    public String build() {
        return stringBuilder.toString();
    }
}

class PreparedSQLBuilder {
    private final SQLBuilder builder;

    public PreparedSQLBuilder(Function<Object, Integer> visitHole) {
        this.builder = new SQLBuilder(arg -> {
            final int arity = visitHole.apply(arg);
            for (int i = 0; i < arity; i++) {
                if (i != 0) this.builder.visitSQLLiteral(",");
                this.builder.visitSQLLiteral("?");
            }

        });
    }

    public void visitSQL(SQL sql) {
        builder.visitSQL(sql);
    }

    public PreparedStatement build(Connection connection) throws SQLException {
        return connection.prepareStatement(builder.build());
    }
}

class UnpreparedSQLBuilder {
    private final SQLBuilder builder;

    public UnpreparedSQLBuilder(Function<Object, List<String>> visitHole) {
        this.builder = new SQLBuilder(arg ->{
            final List<String> xs = visitHole.apply(arg);
            for (int i = 0; i < xs.size(); i++) {
                if (i != 0) this.builder.visitSQLLiteral(",");
                this.builder.visitSQLLiteral(xs.get(i));
            }
        });
    }

    public void visitSQL(SQL sql) {
        builder.visitSQL(sql);
    }

    public String build() {
        return builder.build();
    }
}

class BatchBuilder {
    private final int size;
    private final Write.Context wm;

    private final List<Collection> collections = new ArrayList<>();

    public BatchBuilder(int size, Write.Context wm) {
        this.size = size;
        this.wm = wm;
    }

    public BoundWrite<?> visitHole(Object arg) {
        if (arg instanceof SQL.Hole) {
            final SQL.Hole<?> hole = (SQL.Hole<?>) arg;
            collections.add(Collections.nCopies(size, hole.object));
            return hole.write.bind(wm);
        } else if (arg instanceof SQL.BatchHole) {
            final SQL.BatchHole<?> hole = (SQL.BatchHole<?>) arg;
            collections.add(hole.objects);
            return hole.write.bind(wm);
        } else {
            throw new IllegalStateException("Not expecting " + arg);
        }
    }

    public List<Collection> build() {
        return collections;
    }
}

@ParametersAreNonnullByDefault
class BatchUnpreparedSQLBuilder {
    private BatchUnpreparedSQLBuilder() {}

    public static Map.Entry<Integer, Iterator<String>> build(SQL sql, Write.Context wm) {
        final BatchBuilder batchBuilder = new BatchBuilder(sql.size(), wm);

        final List<Map.Entry<BoundWrite<?>, List<String>>> boundWrites = new ArrayList<>();
        final UnpreparedSQLBuilder sqlBuilder = new UnpreparedSQLBuilder(arg -> {
            final BoundWrite<?> boundWrite = batchBuilder.visitHole(arg);

            final List<String> result = new ArrayList<>();
            for (int i = 0; i < boundWrite.arity(); i++) {
                result.add(UUID.randomUUID().toString());
            }

            boundWrites.add(new AbstractMap.SimpleImmutableEntry<>(boundWrite, result));
            return result;
        });

        sqlBuilder.visitSQL(sql);

        final String sqlString = sqlBuilder.build();
        final List<Collection> batchBuilt = batchBuilder.build();

        final List<Iterator> iterators = batchBuilt.stream().map(Collection::iterator).collect(Collectors.toList());
        return new AbstractMap.SimpleImmutableEntry<>(
                sql.size(),
                new Iterator<String>() {
                    private int i = 0;

                    @Override
                    public boolean hasNext() {
                        return i < sql.size();
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public String next() {
                        String result = sqlString;
                        for (int j = 0; j < boundWrites.size(); j++) {
                            final Map.Entry<BoundWrite<?>, List<String>> boundWrite = boundWrites.get(j);
                            final List<String> replacements = ((BoundWrite<Object>)boundWrite.getKey()).asSQL(iterators.get(j).next());
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

@ParametersAreNonnullByDefault
class BatchPreparedSQLBuilder {
    private interface Action {
        void write(PreparedStatement stmt, IndexRef ref, Object arg) throws SQLException;
    }

    private BatchPreparedSQLBuilder() {}

    public static PreparedStatement build(SQL sql, Write.Context wm, Connection connection) throws SQLException {
        final int size = sql.size();
        final BatchBuilder batch = new BatchBuilder(size, wm);

        final List<Action> actions = new ArrayList<>();
        final PreparedSQLBuilder sqlBuilder = new PreparedSQLBuilder(arg -> {
            final BoundWrite<?> write = batch.visitHole(arg);
            actions.add((s, ix, x) -> ((BoundWrite<Object>)write).set(s, ix, x));
            return write.arity();
        });

        sqlBuilder.visitSQL(sql);

        final PreparedStatement stmt = sqlBuilder.build(connection);
        final List<Collection> batchBuilt = batch.build();

        final List<Iterator> iterators = batchBuilt.stream().map(Collection::iterator).collect(Collectors.toList());
        for (int i = 0; i < size; i++) {
            final IndexRef ref = new IndexRef();
            for (int j = 0; j < iterators.size(); j++) {
                actions.get(j).write(stmt, ref, iterators.get(j).next());
            }
            stmt.addBatch();
        }

        return stmt;
    }
}

class BespokeUnpreparedSQLBuilder {
    private BespokeUnpreparedSQLBuilder() {}

    @SuppressWarnings("unchecked")
    public static String build(SQL sql, Write.Context wm) {
        final UnpreparedSQLBuilder sqlBuilder = new UnpreparedSQLBuilder(arg -> {
            final SQL.Hole<?> hole = BespokePreparedSQLBuilder.unwrapHole(arg);
            return ((SQL.Hole<Object>)hole).write.bind(wm).asSQL(hole.object);
        });

        sqlBuilder.visitSQL(sql);
        return sqlBuilder.build();
    }
}

class BespokePreparedSQLBuilder {
    private interface Action {
        void write(PreparedStatement stmt, IndexRef ref) throws SQLException;
    }

    private BespokePreparedSQLBuilder() {}

    @SuppressWarnings("unchecked")
    public static PreparedStatement build(SQL sql, Write.Context wm, Connection connection) throws SQLException {
        final PreparedSQLBuilder sqlBuilder;
        final List<Action> actions = new ArrayList<>();

        sqlBuilder = new PreparedSQLBuilder(arg -> {
            final SQL.Hole<?> hole = unwrapHole(arg);
            final BoundWrite<?> boundWrite = hole.write.bind(wm);
            actions.add((stmt, ref) -> ((BoundWrite<Object>) boundWrite).set(stmt, ref, hole.object));
            return boundWrite.arity();
        });

        sqlBuilder.visitSQL(sql);
        final PreparedStatement stmt = sqlBuilder.build(connection);

        final IndexRef ref = new IndexRef();
        for (Action action : actions) {
            action.write(stmt, ref);
        }

        return stmt;
    }

    static SQL.Hole<?> unwrapHole(Object arg) {
        if (arg instanceof SQL.Hole) {
            return (SQL.Hole)arg;
        } else if (arg instanceof SQL.BatchHole) {
            throw new IllegalArgumentException("This SQL statement has some batched parts, but you are trying to execute it in unbatched mode");
        } else {
            throw new IllegalStateException("Not expecting " + arg);
        }
    }
}
