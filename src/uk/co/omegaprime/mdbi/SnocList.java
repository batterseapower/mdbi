package uk.co.omegaprime.mdbi;

import java.util.*;

class SnocList<T> implements Iterable<T> {
    private final Optional<SnocList<T>> init;
    private final T last;

    public SnocList(Optional<SnocList<T>> init, T last) {
        this.init = init;
        this.last = last;
    }

    public static <T> SnocList<T> singleton(T last) {
        return new SnocList<>(Optional.empty(), last);
    }

    public SnocList<T> snoc(T last) {
        return new SnocList<>(Optional.of(this), last);
    }

    public SnocList<T> snocs(SnocList<T> xs) {
        SnocList<T> result = this;
        for (T x : xs) {
            result = result.snoc(x);
        }

        return result;
    }

    @Override
    public Iterator<T> iterator() {
        final ArrayDeque<T> ts = new ArrayDeque<>();

        SnocList<T> current = this;
        while (true) {
            ts.push(current.last);

            if (current.init.isPresent()) {
                current = current.init.get();
            } else {
                break;
            }
        }

        return ts.iterator();
    }
}
