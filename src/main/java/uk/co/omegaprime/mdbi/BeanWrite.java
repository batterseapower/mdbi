package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class BeanWrite<T> implements Write<T> {
    private final Method[] getters;
    private final Collection<Write<?>> writes;

    public BeanWrite(Class<T> klass, String... fields) {
        this.getters = Reflection.lookupBeanGetters(klass, Arrays.asList(fields));
        this.writes = Arrays.asList(getters).stream().map(r -> new ContextWrite<>(r.getReturnType())).collect(Collectors.toList());
    }

    public BeanWrite(Class<T> klass, Collection<String> fields, Collection<Write<?>> writes) {
        this.getters = Reflection.lookupBeanSetters(klass, fields);
        this.writes = writes;
        Reflection.checkWritesConformance("Fields " + fields, Arrays.asList(getters).stream().map(Method::getReturnType).collect(Collectors.toList()), writes);
    }

    @Override
    public BoundWrite<T> bind(Context ctxt) {
        final List<BoundWrite<?>> boundWrites = writes.stream().map(w -> w.bind(ctxt)).collect(Collectors.toList());
        return new BoundWrite<T>() {
            @Override
            @SuppressWarnings("unchecked")
            public void set(@Nonnull PreparedStatement s, @Nonnull IndexRef ix, T x) throws SQLException {
                for (int i = 0; i < getters.length; i++) {
                    ((BoundWrite<Object>)boundWrites.get(i)).set(s, ix, Reflection.invokeUnchecked(getters[i], x, new Object[0]));
                }
            }

            @Nonnull
            @Override
            @SuppressWarnings("unchecked")
            public List<String> asSQL(T x) {
                final List<String> result = new ArrayList<>();
                for (int i = 0; i < getters.length; i++) {
                    final Object o;
                    try {
                        o = Reflection.invokeUnchecked(getters[i], x, new Object[0]);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    result.addAll(((BoundWrite<Object>) boundWrites.get(i)).asSQL(o));
                }
                return result;
            }

            @Override
            public int arity() {
                return boundWrites.stream().mapToInt(BoundWrite::arity).sum();
            }
        };
    }
}
