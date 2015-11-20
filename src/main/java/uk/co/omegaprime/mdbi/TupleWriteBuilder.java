package uk.co.omegaprime.mdbi;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Allows the construction of {@link Write} instances that can turn compound Java objects into sequences of SQL values. */
public class TupleWriteBuilder<T> {
    private final List<Write<T>> args = new ArrayList<>();

    private TupleWriteBuilder() { }
    public static <T> TupleWriteBuilder<T> create() { return new TupleWriteBuilder<>(); }

    /** Turns the property specified by {@code f} into a SQL object using the {@link Write} registered in the {@link Context} for the supplied class. */
    public <U> TupleWriteBuilder<T> add(Class<U> klass, Function<T, U> f) {
        return add(new ContextWrite<U>(klass), f);
    }

    /** Turns the property specified by {@code f} into a SQL object using the supplied {@code Write} instance. */
    public <U> TupleWriteBuilder<T> add(Write<U> write, Function<T, U> f) {
        args.add(Writes.map(write, f));
        return this;
    }

    public Write<T> build() {
        return new TupleWrite<>(new ArrayList<>(args));
    }
}
