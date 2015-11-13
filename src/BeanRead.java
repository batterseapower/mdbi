import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BeanRead<T> implements Read<T> {
    private final Class<T> klass;
    private final Constructor<T> constructor;
    private final Method[] setters;
    private final List<Read<?>> reads;

    public BeanRead(Class<T> klass, String... fields) {
        this.klass = klass;
        this.constructor = Reflection.getBeanConstructor(klass);
        this.setters = lookupBeanSetters(klass, Arrays.asList(fields));
        this.reads = Arrays.asList(this.setters).stream().map(m -> new ContextRead<>((Class<Object>) (m.getParameterTypes()[0]))).collect(Collectors.toList());
    }

    public BeanRead(Class<T> klass, List<String> fields, List<Read<?>> reads) {
        this.klass = klass;
        this.constructor = Reflection.getBeanConstructor(klass);
        this.setters = lookupBeanSetters(klass, fields);
        this.reads = reads;

        Reflection.checkReadsConformance("Fields " + fields, reads.stream().map((Function<Read<?>, Class<?>>) Read::getElementClass).collect(Collectors.toList()), reads);
    }

    private static Method[] lookupBeanSetters(Class<?> klass, List<String> fields) {
        final HashMap<String, Method> setters = new HashMap<>();
        for (Method m : klass.getMethods()) {
            if (m.getName().startsWith("set") && m.getParameterCount() == 1) {
                if (setters.put(m.getName().substring(3), m) != null) {
                    throw new IllegalArgumentException("Class " + klass + " has multiple 1-arg methods called " + m.getName());
                }
            }
        }

        final Method[] methods = new Method[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            final String field = fields.get(i);
            final Method setter = setters.get(field);
            if (setter == null) {
                throw new IllegalArgumentException("Class " + klass + " doesn't have a setter for " + field);
            }
            methods[i] = setter;
        }

        return methods;
    }

    @Override
    public Class<T> getElementClass() {
        return klass;
    }

    @Override
    public BoundRead<T> bind(Map ctxt) {
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
