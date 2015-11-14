import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class TupleWrite<T> implements Write<T> {
    private final List<java.util.Map.Entry<Write<?>, Function<T, ?>>> args;

    private TupleWrite(List<java.util.Map.Entry<Write<?>, Function<T, ?>>> args) {
        this.args = args;
    }

    public static class Builder<T> {
        private final List<java.util.Map.Entry<Write<?>, Function<T, ?>>> args = new ArrayList<>();

        private Builder() { }
        public static <T> Builder<T> create() { return new Builder<>(); }

        public <U> Builder<T> add(Class<U> klass, Function<T, U> f) {
            return add(new ContextWrite<U>(klass), f);
        }

        public <U> Builder<T> add(Write<U> write, Function<T, U> f) {
            args.add(new AbstractMap.SimpleImmutableEntry<>(write, f));
            return this;
        }

        public TupleWrite<T> build() {
            return new TupleWrite<>(new ArrayList<>(args));
        }
    }

    @Override
    public BoundWrite<T> bind(Writes.Map ctxt) {
        final List<BoundWrite<?>> boundWrites = new ArrayList<>(args.size());
        for (java.util.Map.Entry<Write<?>, Function<T, ?>> e : args) {
            boundWrites.add(e.getKey().bind(ctxt));
        }
        return new BoundWrite<T>() {
            @Override
            public int arity() {
                return boundWrites.stream().mapToInt(BoundWrite::arity).sum();
            }

            @Override
            @SuppressWarnings("unchecked")
            public void set(@Nonnull PreparedStatement s, @Nonnull IndexRef ix, T x) throws SQLException {
                for (int i = 0; i < boundWrites.size(); i++) {
                    ((BoundWrite<Object>)boundWrites.get(i)).set(s, ix, args.get(i).getValue().apply(x));
                }
            }

            @Nonnull
            @Override
            @SuppressWarnings("unchecked")
            public List<String> asSQL(T x) {
                final List<String> result = new ArrayList<>();
                for (int i = 0; i < boundWrites.size(); i++) {
                    result.addAll(((BoundWrite<Object>)boundWrites.get(i)).asSQL(args.get(i).getValue().apply(x)));
                }
                return result;
            }
        };
    }
}
