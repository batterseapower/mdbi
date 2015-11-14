import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BeanRead<T> implements Read<T> {
    private final Class<T> klass;
    private final Constructor<T> constructor;
    private final Method[] setters;
    private final List<Read<?>> reads;

    @SuppressWarnings("unchecked")
    public BeanRead(Class<T> klass, String... fields) {
        this.klass = klass;
        this.constructor = Reflection.getBeanConstructor(klass);
        this.setters = Reflection.lookupBeanSetters(klass, Arrays.asList(fields));
        this.reads = Arrays.asList(this.setters).stream().map(m -> new ContextRead<>((Class<Object>) (m.getParameterTypes()[0]))).collect(Collectors.toList());
    }

    public BeanRead(Class<T> klass, List<String> fields, List<Read<?>> reads) {
        this.klass = klass;
        this.constructor = Reflection.getBeanConstructor(klass);
        this.setters = Reflection.lookupBeanSetters(klass, fields);
        this.reads = reads;

        Reflection.checkReadsConformance("Fields " + fields, Arrays.asList(setters).stream().map(m -> m.getParameterTypes()[0]).collect(Collectors.toList()), reads);
    }

    @Override
    public Class<T> getElementClass() {
        return klass;
    }

    @Override
    public BoundRead<T> bind(Reads.Map ctxt) {
        final List<BoundRead> boundReads = reads.stream().map(r -> r.bind(ctxt)).collect(Collectors.toList());
        return new BoundRead<T>() {
            @Override
            public T get(ResultSet rs, IndexRef ix) throws SQLException {
                final T x = Reflection.constructUnchecked(constructor, new Object[0]);
                for (int i = 0; i < setters.length; i++) {
                    Reflection.invokeUnchecked(setters[i], x, new Object[]{boundReads.get(i).get(rs, ix)});
                }
                return x;
            }
        };
    }
}
