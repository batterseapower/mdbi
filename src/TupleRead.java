import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TupleRead<T> implements Read<T> {
    private final Class<T> klass;
    private final Constructor<T> constructor;
    private final List<Read<?>> reads;

    public TupleRead(Class<T> klass) {
        this.klass = klass;
        this.constructor = Reflection.getConstructor(klass);
        this.reads = Arrays.asList(constructor.getParameterTypes()).stream().map(ContextRead::new).collect(Collectors.toList());
    }

    public TupleRead(Class<T> klass, List<Read<?>> reads) {
        this.klass = klass;
        this.constructor = Reflection.getConstructor(klass);
        this.reads = reads;

        Reflection.checkReadsConformance("Constructor " + constructor, Arrays.asList(constructor.getParameterTypes()), reads);
    }

    @Override
    public Class<T> getElementClass() {
        return klass;
    }

    @Override
    public BoundRead<T> bind(Reads.Map ctxt) {
        final List<BoundRead<?>> boundReads = reads.stream().map(r -> r.bind(ctxt)).collect(Collectors.toList());
        return (rs, ix) -> {
            final Object[] arguments = new Object[boundReads.size()];
            for (int i = 0; i < arguments.length; i++) {
                arguments[i] = boundReads.get(i).get(rs, ix);
            }
            return Reflection.constructUnchecked(constructor, arguments);
        };
    }
}
