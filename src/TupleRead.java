import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TupleRead<T> implements Read<T> {
    private final Class<T> klass;
    private final Constructor<T> constructor;
    private final List<Read<?>> reads;

    public TupleRead(Class<T> klass) {
        this.klass = klass;
        this.constructor = getConstructor(klass);
        this.reads = Arrays.asList(constructor.getParameterTypes()).stream().map(ContextRead::new).collect(Collectors.toList());
    }

    public TupleRead(Class<T> klass, List<Read<?>> reads) {
        this.klass = klass;
        this.constructor = getConstructor(klass);

        if (reads.size() != constructor.getParameterCount()) {
            throw new IllegalArgumentException("Constructor has " + constructor.getParameterCount() + " arguments but you supplied " + reads.size() + " readers");
        }

        final Class<?>[] types = constructor.getParameterTypes();
        for (int i = 0; i < reads.size(); i++) {
            if (!types[i].isAssignableFrom(reads.get(i).getElementClass())) {
                throw new IllegalArgumentException("Constructor param " + i + " is of type " + types[i] + " but you supplied a reader for " + reads.get(i).getElementClass());
            }
        }

        this.reads = reads;
    }

    @SuppressWarnings("unchecked")
    private static <T> Constructor<T> getConstructor(Class<T> klass) {
        final Constructor<?>[] constructors = klass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalArgumentException("No public constructors for " + klass);
        } else if (constructors.length > 1) {
            throw new IllegalArgumentException("Ambiguous public constructor for " + klass);
        } else {
            return (Constructor<T>)constructors[0];
        }
    }

    @Override
    public Class<T> getElementClass() {
        return klass;
    }

    @Override
    public BoundRead<T> bind(Read.Map ctxt) {
        final List<BoundRead<?>> boundReads = reads.stream().map(r -> r.bind(ctxt)).collect(Collectors.toList());
        return (rs, ix) -> {
            final Object[] arguments = new Object[boundReads.size()];
            for (int i = 0; i < arguments.length; i++) {
                arguments[i] = boundReads.get(i).get(rs, ix);
            }
            try {
                return constructor.newInstance(arguments);
            } catch (InstantiationException e) {
                throw new IllegalStateException("Constructor " + constructor + " was not callable, though we should have already checked that", e);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Constructor " + constructor + " was not accessible, though we should have already checked that", e);
            } catch (InvocationTargetException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException)cause;
                } else {
                    throw new UndeclaredThrowableException(cause);
                }
            }
        };
    }
}
