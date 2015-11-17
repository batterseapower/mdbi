package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

class TupleWrite<T> implements Write<T> {
    private final List<Write<T>> args;

    public TupleWrite(List<Write<T>> args) {
        this.args = args;
    }

    @Override
    public BoundWrite<T> bind(Write.Context ctxt) {
        final List<BoundWrite<? super T>> boundWrites = new ArrayList<>(args.size());
        for (Write<T> write : args) {
            boundWrites.add(write.bind(ctxt));
        }
        return new BoundWrite<T>() {
            @Override
            public int arity() {
                return boundWrites.stream().mapToInt(BoundWrite::arity).sum();
            }

            @Override
            public void set(@Nonnull PreparedStatement s, @Nonnull IndexRef ix, T x) throws SQLException {
                for (BoundWrite<? super T> write : boundWrites) {
                    write.set(s, ix, x);
                }
            }

            @Nonnull
            @Override
            public List<String> asSQL(T x) {
                final List<String> result = new ArrayList<>();
                for (BoundWrite<? super T> write : boundWrites) {
                    result.addAll(write.asSQL(x));
                }
                return result;
            }
        };
    }
}
