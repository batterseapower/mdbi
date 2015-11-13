import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BeanWrite<T> implements Write<T> {
    private final Method[] getters;
    private final List<Write<?>> writes;

    @SuppressWarnings("unchecked")
    public BeanWrite(Class<T> klass, String... fields) {
        this.getters = Reflection.lookupBeanGetters(klass, Arrays.asList(fields));
        this.writes = Arrays.asList(getters).stream().map(r -> new ContextWrite<>((Class<Object>)r.getReturnType())).collect(Collectors.toList());
    }

    public BeanWrite(Class<T> klass, List<String> fields, List<Write<?>> writes) {
        this.getters = Reflection.lookupBeanSetters(klass, fields);
        this.writes = writes;
        Reflection.checkWritesConformance("Fields " + fields, Arrays.asList(getters).stream().map((Function<Method, Class<?>>) Method::getReturnType).collect(Collectors.toList()), writes);
    }

    @Override
    @SuppressWarnings("unchecked")
    public BoundWrite<T> bind(Map ctxt) {
        final List<BoundWrite<?>> boundWrites = writes.stream().map(w -> w.bind(ctxt)).collect(Collectors.toList());
        return new BoundWrite<T>() {
            @Override
            public void set(PreparedStatement s, IndexRef ix, T x) throws SQLException {
                for (int i = 0; i < getters.length; i++) {
                    ((BoundWrite<Object>)boundWrites.get(i)).set(s, ix, Reflection.invokeUnchecked(getters[i], x, new Object[0]));
                }
            }

            @Override
            public List<String> asSQL(T x) {
                final List<String> result = new ArrayList<>();
                for (int i = 0; i < getters.length; i++) {
                    result.addAll(((BoundWrite<Object>)boundWrites.get(i)).asSQL(Reflection.invokeUnchecked(getters[i], x, new Object[0])));
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
