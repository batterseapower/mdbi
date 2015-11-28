package uk.co.omegaprime.mdbi;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.*;

class Reflection {
    private Reflection() {}

    public static <T> Constructor<T> getUniqueConstructor(Class<T> klass) {
        final Constructor<?>[] constructors = klass.getConstructors();
        return chooseUniqueConstructor(klass, constructors);
    }

    @SuppressWarnings("unchecked")
    private static <T> Constructor<T> chooseUniqueConstructor(Class<T> klass, Constructor<?>[] constructors) {
        if (constructors.length == 0) {
            throw new IllegalArgumentException("No public constructors for " + klass);
        } else if (constructors.length > 1) {
            throw new IllegalArgumentException("Ambiguous public constructor for " + klass);
        } else {
            return (Constructor<T>)constructors[0];
        }
    }

    public static <T> Constructor<T> getCompatibleConstructor(Class<T> klass, List<Class<?>> klasses) {
        final Constructor<?>[] constructors = klass.getConstructors();
        final List<Constructor<?>> candidates = new ArrayList<>();

        // Sort of a crummy implementation of the overload rules: http://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.12.2
        // It might be worth taking a dependency just to get this right. It's hard to do, and it's not our core competency.
        for (Constructor<?> constructor : constructors) {
            final Class<?>[] params = constructor.getParameterTypes();
            if (params.length != klasses.size()) continue;

            boolean compatible = true;
            for (int i = 0; i < params.length; i++) {
                if (!params[i].isAssignableFrom(klasses.get(i))) {
                    compatible = false;
                    break;
                }
            }

            if (!compatible) continue;

            candidates.add(constructor);
        }

        boolean changed = true;
        while (changed) {
            changed = false;

            final Iterator<Constructor<?>> it = candidates.iterator();
            while (it.hasNext()) {
                final Constructor<?> constructor = it.next();
                final Class<?>[] params = constructor.getParameterTypes();

                boolean atLeastOneOtherConstructorIsAtLeastAsSpecific = false;
                for (Constructor<?> otherConstructor : candidates) {
                    if (otherConstructor == constructor) continue;

                    final Class<?>[] otherParams = otherConstructor.getParameterTypes();
                    if (otherParams.length != params.length) continue;

                    boolean otherConstructorIsAtLeastAsSpecific = true;
                    for (int i = 0; i < params.length; i++) {
                        if (!params[i].isAssignableFrom(otherParams[i])) {
                            // We can accept something that "other" could not, so we are more specific
                            otherConstructorIsAtLeastAsSpecific = false;
                            break;
                        }
                    }

                    if (otherConstructorIsAtLeastAsSpecific) {
                        // All of params[i] are assignable from otherParams[i]
                        atLeastOneOtherConstructorIsAtLeastAsSpecific = true;
                        break;
                    }
                }

                if (atLeastOneOtherConstructorIsAtLeastAsSpecific) {
                    changed = true;
                    it.remove();
                }
            }
        }

        return chooseUniqueConstructor(klass, candidates.toArray(new Constructor<?>[candidates.size()]));
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

    public static void checkReadsConformance(String context, List<Class<?>> types, Collection<Read<?>> reads) {
        if (reads.size() != types.size()) {
            throw new IllegalArgumentException(context + " has " + types.size() + " elements but you supplied " + reads.size() + " readers");
        }

        final Iterator<Read<?>> readsIt = reads.iterator();
        for (int i = 0; i < reads.size(); i++) {
            final Read<?> read = readsIt.next();
            if (!types.get(i).isAssignableFrom(read.getElementClass())) {
                throw new IllegalArgumentException(context + " element " + i + " is of type " + types.get(i) + " but you supplied a reader for " + read.getElementClass());
            }
        }
    }

    public static void checkWritesConformance(String context, List<Class<?>> types, Collection<Write<?>> writes) {
        if (writes.size() != types.size()) {
            throw new IllegalArgumentException(context + " has " + types.size() + " elements but you supplied " + writes.size() + " writers");
        }
    }

    public static Method[] lookupBeanSetters(Class<?> klass, Collection<String> fields) {
        final HashMap<String, Method> setters = new HashMap<>();
        for (Method m : klass.getMethods()) {
            if (m.getName().startsWith("set") && m.getParameterCount() == 1) {
                if (setters.put(m.getName().substring(3), m) != null) {
                    throw new IllegalArgumentException("Class " + klass + " has multiple 1-arg methods called " + m.getName());
                }
            }
        }

        final Method[] methods = new Method[fields.size()];
        final Iterator<String> fieldsIt = fields.iterator();
        for (int i = 0; i < fields.size(); i++) {
            final String field = fieldsIt.next();
            final Method setter = setters.get(field);
            if (setter == null) {
                throw new IllegalArgumentException("Class " + klass + " doesn't have a setter for " + field);
            }
            methods[i] = setter;
        }

        return methods;
    }

    public static <T> Method[] lookupBeanGetters(Class<T> klass, List<String> fields) {
        final HashMap<String, Method> getters = new HashMap<>();
        for (Method m : klass.getMethods()) {
            if (m.getName().startsWith("is") && m.getParameterCount() == 0) {
                final String property = m.getName().substring(2);
                if (getters.put(property, m) != null) {
                    throw new IllegalArgumentException("Class " + klass + " has multiple getters for property " + property);
                }
            } else if (m.getName().startsWith("get") && m.getParameterCount() == 0) {
                final String property = m.getName().substring(3);
                if (getters.put(property, m) != null) {
                    throw new IllegalArgumentException("Class " + klass + " has multiple getters for property " + property);
                }
            }
        }

        final Method[] methods = new Method[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            final String field = fields.get(i);
            final Method setter = getters.get(field);
            if (setter == null) {
                throw new IllegalArgumentException("Class " + klass + " doesn't have a getter for " + field);
            }
            methods[i] = setter;
        }

        return methods;
    }
}
