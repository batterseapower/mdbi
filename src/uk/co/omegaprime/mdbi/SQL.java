package uk.co.omegaprime.mdbi;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;

@ParametersAreNonnullByDefault
public final class SQL {
    static class Hole<T> {
        public final @Nullable T object;
        public final Write<T> write;

        Hole(@Nullable T object, Write<T> write) {
            this.object = object;
            this.write = write;
        }
    }

    static class BatchHole<T> {
        public final Collection<T> objects;
        public final Write<T> write;

        BatchHole(Collection<T> objects, Write<T> write) {
            this.objects = objects;
            this.write = write;
        }
    }

    // Elements are either Hole, BatchHole, or String
    // TODO: avoid copying this so much
    final List<Object> args;
    final @Nullable Integer size;

    SQL(List<Object> args, @Nullable Integer size) {
        this.args = args;
        this.size = size;
    }

    int size() {
        return this.size == null ? 0 : this.size;
    }

    @SuppressWarnings("unchecked")
    public SQL $(@Nullable Object x) {
        return x == null ? $(Writes.nullReference(),      null)
                         : $((Class<Object>)x.getClass(), x);
    }

    public <T> SQL $(Class<T> klass, @Nullable T x) {
        return snoc(new Hole<>(x, new ContextWrite<>(klass)), size);
    }

    public <T> SQL $(Write<T> write, @Nullable T x) {
        return snoc(new Hole<>(x, write), size);
    }

    @SuppressWarnings("unchecked")
    public <T> SQL $s(Collection<T> arg) {
        final Class<?> klass;
        if (arg.size() > 0) {
            final Iterator<T> it = arg.iterator();
            T example = it.next();
            while (example == null && it.hasNext()) {
                example = it.next();
            }

            klass = example == null ? null : example.getClass();
        } else {
            klass = null;
        }

        if (klass == null) {
            // We know for sure that all elements of the column are null, this is the best we can do..
            return $s(Writes.nullReference(), arg);
        } else {
            return $s((Class<T>)klass, arg);
        }
    }

    public <T> SQL $s(Class<T> klass, Collection<T> x) {
        return $s(Writes.useContext(klass), x);
    }

    public <T> SQL $s(Write<T> write, Collection<T> x) {
        if (size != null && size != x.size()) {
            throw new IllegalArgumentException("All collections supplied to a batch SQL statement must be of the same size, but you had both sizes " + size + " and " + x.size());
        }

        return snoc(new BatchHole<>(x, write), x.size());
    }

    public SQL sql(SQL x) {
        return snocs(x.args, size);
    }

    public SQL sql(String x) {
        return sql(MJDBC.sql(x));
    }

    @SafeVarargs
    public final <T> SQL in(T... xs) {
        // Exploit the fact that 'null not in (null)' to avoid generating nullary IN clauses:
        // systems like SQL Server can't parse them
        SQL result = sql("in (null");
        for (T x : xs) {
            result = result.sql(",").$(x);
        }

        return result.sql(")");
    }

    public SQL in(SQL... xs) {
        // Exploit the fact that 'null not in (null)' to avoid generating nullary IN clauses:
        // systems like SQL Server can't parse them
        SQL result = sql("in (null");
        for (SQL x : xs) {
            result = result.sql(",").sql(x);
        }

        return result.sql(")");
    }

    private SQL snoc(Object x, @Nullable Integer size) {
        final List<Object> newArgs = new ArrayList<>(args);
        newArgs.add(x);
        return new SQL(newArgs, size);
    }

    private SQL snocs(List<Object> x, @Nullable Integer size) {
        final List<Object> newArgs = new ArrayList<>(args);
        newArgs.addAll(x);
        return new SQL(newArgs, size);
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder();
        for (Object arg : args) {
            if (arg instanceof String) {
                result.append((String)arg);
            } else if (arg instanceof Hole) {
                result.append("${").append(((Hole)arg).object).append("}");
            } else if (arg instanceof BatchHole) {
                result.append("$${").append(((BatchHole)arg).objects).append("}");
            } else {
                throw new IllegalStateException("Unexpected arg " + arg);
            }
        }

        return result.toString();
    }
}
