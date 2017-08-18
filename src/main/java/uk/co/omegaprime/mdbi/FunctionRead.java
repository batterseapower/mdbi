package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class FunctionRead<T> implements Read<T> {
    private static Method getMethod(Object x) {
        final List<Method> candidates = new ArrayList<>();
        for (Method method : x.getClass().getDeclaredMethods()) {
            final int modifiers = method.getModifiers();
            if (Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers)) {
                candidates.add(method);
            }
        }

        if (candidates.size() != 1) {
            throw new IllegalArgumentException("The object you supply to Reads.ofFunction must have " +
                    "exactly one public instance method, but we found " + candidates);
        }

        return candidates.get(0);
    }

    private final Method method;
    private final Class<? extends T> klass; // INVARIANT: equal to the return type of "method"
    private final Object receiver;
    private final Collection<Read<?>> reads;

    public FunctionRead(Class<? extends T> klass, Object receiver) {
        this.method = getMethod(receiver);
        this.klass = checkExtends(method.getReturnType(), klass);
        this.receiver = receiver;
        this.reads = Arrays.asList(method.getParameterTypes()).stream().map(ContextRead::new).collect(Collectors.toList());

        // You might be surprised that I have do to this, but it's necessary if e.g. the receiver is a package-private class
        // (which it normally will be, if it's an instance of an anonymous inner class!)
        method.setAccessible(true);
    }

    public FunctionRead(Class<? extends T> klass, Object receiver, Collection<Read<?>> reads) {
        this.method = getMethod(receiver);
        this.klass = checkExtends(method.getReturnType(), klass);
        this.receiver = receiver;
        this.reads = reads;

        method.setAccessible(true);
        Reflection.checkReadsConformance("Method " + method, Arrays.asList(method.getParameterTypes()), reads);
    }

    private static <T> Class<? extends T> checkExtends(Class<?> klass, Class<T> mustExtend) {
        if (!mustExtend.isAssignableFrom(klass)) {
            throw new IllegalArgumentException("Found class " + klass + " must extend supplied class " + mustExtend);
        } else {
            // We know this is safe because mustExtend is assignable from klass, so klass must extend mustExtend
            //noinspection unchecked
            return (Class<? extends T>)klass;
        }
    }

    @Override
    public Class<? extends T> getElementClass() {
        return klass;
    }

    @Override
    public BoundRead<? extends T> bind(Read.Context ctxt) {
        final List<BoundRead<?>> boundReads = reads.stream().map(r -> r.bind(ctxt)).collect(Collectors.toList());
        return new BoundRead<T>() {
            @Override
            public int arity() {
                return boundReads.stream().mapToInt(BoundRead::arity).sum();
            }

            @Override
            public T get(@Nonnull ResultSet rs, @Nonnull IndexRef ix) throws SQLException {
                final Object[] arguments = new Object[boundReads.size()];
                for (int i = 0; i < arguments.length; i++) {
                    arguments[i] = boundReads.get(i).get(rs, ix);
                }
                return klass.cast(Reflection.invokeUnchecked(method, receiver, arguments));
            }
        };
    }
}
