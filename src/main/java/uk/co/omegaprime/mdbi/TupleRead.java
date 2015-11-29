package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class TupleRead<T> implements Read<T> {
    private final Class<T> klass;
    private final Constructor<T> constructor;
    private final Collection<Read<?>> reads;

    public TupleRead(Class<T> klass) {
        this.klass = klass;
        this.constructor = Reflection.getUniqueConstructor(klass);
        this.reads = Arrays.asList(constructor.getParameterTypes()).stream().map(ContextRead::new).collect(Collectors.toList());
    }

    public TupleRead(Class<T> klass, Collection<Read<?>> reads) {
        this.klass = klass;
        this.constructor = Reflection.getCompatibleConstructor(klass, reads.stream().map(Read::getElementClass).collect(Collectors.toList()));
        this.reads = reads;
    }

    @Override
    public Class<T> getElementClass() {
        return klass;
    }

    @Override
    public BoundRead<T> bind(Read.Context ctxt) {
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
                return Reflection.constructUnchecked(constructor, arguments);
            }
        };
    }
}
