package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiFunction;

/** An immutable type representing a SQL statement with zero or more holes that are filled by Java objects */
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

    // Elements of list are either Hole, BatchHole, or String
    final SnocList<Object> args;
    final @Nullable Integer size;

    SQL(SnocList<Object> args, @Nullable Integer size) {
        this.args = args;
        this.size = size;
    }

    int size() {
        return this.size == null ? 0 : this.size;
    }

    /** Interpolate a Java object into the SQL query using a {@link Write} instance that is appropriate for its runtime type */
    @SuppressWarnings("unchecked")
    public SQL $(@Nullable Object x) {
        return x == null ? $(Writes.nullReference(),      null)
                         : $((Class<Object>) x.getClass(), x);
    }

    /** Interpolate a Java object into the SQL query using a {@link Write} instance suitable for the supplied class */
    public <T> SQL $(Class<T> klass, @Nullable T x) {
        return new SQL(args.snoc(new Hole<>(x, new ContextWrite<>(klass))), size);
    }

    /** Interpolate a Java object into the SQL query using the supplied {@link Write} instance */
    public <T> SQL $(Write<T> write, @Nullable T x) {
        return new SQL(args.snoc(new Hole<>(x, write)), size);
    }

    /**
     * Interpolate a series of Java objects into a batch SQL query using a {@link Write} instance inferred from the
     * runtime type of the first non-null item in the collection.
     */
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

    /** Interpolate a series of Java objects into a batch SQL query using a {@link Write} instance suitable for the supplied class. */
    public <T> SQL $s(Class<T> klass, Collection<T> x) {
        return $s(Writes.useContext(klass), x);
    }

    /** Interpolate a series of Java objects into a batch SQL query using the supplied {@link Write} instance */
    public <T> SQL $s(Write<T> write, Collection<T> x) {
        if (size != null && size != x.size()) {
            throw new IllegalArgumentException("All collections supplied to a batch SQL statement must be of the same size, but you had both sizes " + size + " and " + x.size());
        }

        return new SQL(args.snoc(new BatchHole<>(x, write)), x.size());
    }

    /** Append a SQL literal */
    public SQL sql(SQL x) {
        return new SQL(args.snocs(x.args), size);
    }

    /** Append a SQL literal */
    public SQL sql(String x) {
        return sql(MDBI.sql(x));
    }

    /**
     * Appends several bits of SQL.
     * <p>
     * The arguments must either be {@code SQL} instances, or Strings (in which case they will be
     * assumed to represent SQL fragments, rather than string parameters to the query).
     */
    public SQL sql(Object... xs) {
        SQL result = sql("");
        for (int i = 0; i < xs.length; i++) {
            final Object x = xs[i];
            if (x == null) {
                throw new NullPointerException("The argument at index " + i + " was null");
            }

            if (x instanceof String) {
                result = result.sql((String)x);
            } else if (x instanceof SQL) {
                result = result.sql((SQL)x);
            } else {
                throw new IllegalArgumentException("Supplied argument " + x + " at index " + i + " is neither a String nor a SQL instance");
            }
        }

        return result;
    }

    /** Append a SQL literal */
    @SafeVarargs
    public final <T> SQL in(T... xs) {
        return in(Arrays.<T>asList(xs));
    }

    /**
     * Append an &quot;IN&quot; clause based on the supplied collection
     *
     * The input objects are turned into SQL using a {@link Write} based on their runtime type, similar to how
     * {@link #$(Object)} works.
     */
    public <T> SQL in(Iterable<T> xs) {
        return inCore(xs, SQL::$);
    }

    /** Append an &quot;IN&quot; clause based on the supplied collection, turning objects into SQL using the {@link Write} instance for the supplied class. */
    public <T> SQL in(Class<T> klass, Iterable<T> xs) {
        return in(Writes.useContext(klass), xs);
    }

    /** Append an &quot;IN&quot; clause based on the supplied collection, turning objects into SQL using the supplied {@link Write} instance. */
    public <T> SQL in(Write<T> write, Iterable<T> xs) {
        return inCore(xs, (sql, x) -> sql.$(write, x));
    }

    private <T> SQL inCore(Iterable<T> xs, BiFunction<SQL, T, SQL> f) {
        final Iterator<T> it = xs.iterator();
        if (!it.hasNext()) {
            // I used to get clever in this case and generate "in (null)" on the basis that nothing is equal
            // to null... unfortunately it seems that e.g. on SQLite, all of these queries return 0 results:
            //   select 1 where 1 in (null)
            //   select 1 where 1 not in (null)
            //   select 1 where not (1 in (null))
            //   select 1 where not (1 not in (null))
            // See also http://stackoverflow.com/questions/129077/not-in-clause-and-null-values
            //
            // Then I tried generating "in ('e0afa0da0e3444d5ae3b34202b759e0c')", where that value is just a
            // random GUID. Unfortunately some systems (e.g. SQL Server) insist on coercing the values in the
            // 'in' clause to the type of the LHS, which obviously fails in this case if e.g. LHS is an int.
            //
            // So now I use a cheeky sub-query:
            return sql(" in (select null where 1 = 0)");
        } else {
            SQL result = f.apply(sql(" in ("), it.next());
            while (it.hasNext()) {
                result = f.apply(result.sql(","), it.next());
            }
            return result.sql(") ");
        }
    }

    static SQL commaSeparate(Iterator<SQL> it) {
        if (!it.hasNext()) throw new IllegalStateException("You must add at least one column");

        SQL result = it.next();
        while (it.hasNext()) {
            result = result.sql(", ").sql(it.next());
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String toString() {
        boolean isBatch = false;
        for (Object arg : args) {
            if (arg instanceof BatchHole) {
                isBatch = true;
                break;
            }
        }

        // We try to be a bit clever to make things easier for the humans looking at SQL objects: we show holes using
        // the proper SQL interpretation if possible. If this fails for whatever reason we splice them in using a made-up syntax.
        final Write.Context tolerantWriteContext = new Write.Context() {
            @Override
            public <T> Write<? super T> get(Class<T> klass) {
                Write<? super T> unbound_ = null;
                try {
                    unbound_ = Context.DEFAULT.writeContext().get(klass);
                } catch (Exception _ignored) {
                    // Will just fallback on default printing method
                }

                final Write<? super T> unbound = unbound_;
                return ctxt -> {
                    BoundWrite<? super T> bound_ = null;
                    if (unbound != null) {
                        try {
                            bound_ = unbound.bind(ctxt);
                        } catch (Exception _ignored) {
                            // Will just fallback on default printing method
                        }
                    }

                    final BoundWrite<? super T> bound = bound_;
                    return new BoundWrite<T>() {
                        private Integer reportedArity;

                        @Override
                        public int arity() {
                            if (reportedArity == null) {
                                if (bound != null) {
                                    try {
                                        reportedArity = bound.arity();
                                    } catch (Exception _ignored) {
                                        // Will just fallback on default printing method
                                    }
                                }

                                if (reportedArity == null) {
                                    reportedArity = 1;
                                }
                            }

                            return reportedArity;
                        }

                        @Override
                        public void set(@Nonnull PreparedStatement s, @Nonnull IndexRef ix, @Nullable T x) throws SQLException {
                            throw new UnsupportedOperationException("This code should be unreachable");
                        }

                        @Nonnull
                        @Override
                        public List<String> asSQL(@Nullable T x) {
                            List<String> result = null;
                            if (bound != null) {
                                try {
                                    result = bound.asSQL(x);
                                } catch (Exception _ignored) {
                                    // Will just fallback on default printing method for this param
                                }
                            }

                            if (result != null && (reportedArity == null || reportedArity == result.size())) {
                                // Common case
                                return result;
                            } else {
                                final int needArity = reportedArity == null ? 1 : reportedArity;
                                final List<String> modifiedResult = new ArrayList<>();
                                int i = 0;
                                while (modifiedResult.size() < needArity) {
                                    if (result != null && i < result.size()) {
                                        modifiedResult.add(result.get(i++));
                                    } else {
                                        modifiedResult.add("${" + x + (needArity == 1 ? "" : ":" + modifiedResult.size()) + "}");
                                    }
                                }

                                return modifiedResult;
                            }
                        }
                    };
                };
            }
        };

        if (isBatch) {
            final StringBuilder result = new StringBuilder();
            final Iterator<String> it = BatchUnpreparedSQLBuilder.build(this, tolerantWriteContext).getValue();
            while (it.hasNext()) {
                if (result.length() != 0) result.append("\n");
                result.append(it.next());
            }

            return result.toString();
        } else {
            return BespokeUnpreparedSQLBuilder.build(this, tolerantWriteContext);
        }
    }
}
