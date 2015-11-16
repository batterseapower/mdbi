package uk.co.omegaprime.mdbi;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TupleWriteBuilder<T> {
    private final List<Write<T>> args = new ArrayList<>();

    private TupleWriteBuilder() { }
    public static <T> TupleWriteBuilder<T> create() { return new TupleWriteBuilder<>(); }

    public <U> TupleWriteBuilder<T> add(Class<U> klass, Function<T, U> f) {
        return add(new ContextWrite<U>(klass), f);
    }

    public <U> TupleWriteBuilder<T> add(Write<U> write, Function<T, U> f) {
        args.add(Writes.map(write, f));
        return this;
    }

    public TupleWrite<T> build() {
        return new TupleWrite<>(new ArrayList<>(args));
    }
}
