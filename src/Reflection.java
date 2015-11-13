import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;

class Reflection {
    private Reflection() {}

    @SuppressWarnings("unchecked")
    public static <T> Constructor<T> getConstructor(Class<T> klass) {
        final Constructor<?>[] constructors = klass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalArgumentException("No public constructors for " + klass);
        } else if (constructors.length > 1) {
            throw new IllegalArgumentException("Ambiguous public constructor for " + klass);
        } else {
            return (Constructor<T>)constructors[0];
        }
    }

    public static <T> Constructor<T> getBeanConstructor(Class<T> klass) {
        try {
            return klass.getConstructor(new Class[0]);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Class " + klass + " must have a public no-args constructor");
        }
    }

    public static <T> T constructUnchecked(Constructor<T> constructor, Object[] arguments) {
        try {
            return constructor.newInstance(arguments);
        } catch (InstantiationException e) {
            throw new IllegalStateException("Constructor " + constructor + " was not callable, though we should have already checked that", e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Constructor " + constructor + " was not accessible, though we should have already checked that", e);
        } catch (InvocationTargetException e) {
            throw rethrowInvocationTargetException(e);
        }
    }

    public static Object invokeUnchecked(Method method, Object receiver, Object[] arguments) {
        try {
            return method.invoke(receiver, arguments);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Method " + method + " was not accessible, though we should have already checked that", e);
        } catch (InvocationTargetException e) {
            throw rethrowInvocationTargetException(e);
        }
    }

    private static RuntimeException rethrowInvocationTargetException(InvocationTargetException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
            return (RuntimeException)cause;
        } else {
            return new UndeclaredThrowableException(cause);
        }
    }

    public static void checkReadsConformance(String context, List<Class<?>> types, List<Read<?>> reads) {
        if (reads.size() != types.size()) {
            throw new IllegalArgumentException(context + " has " + types.size() + " arguments but you supplied " + reads.size() + " readers");
        }

        for (int i = 0; i < reads.size(); i++) {
            if (!types.get(i).isAssignableFrom(reads.get(i).getElementClass())) {
                throw new IllegalArgumentException(context + " element " + i + " is of type " + types.get(i) + " but you supplied a reader for " + reads.get(i).getElementClass());
            }
        }
    }
}
