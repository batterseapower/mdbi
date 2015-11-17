package uk.co.omegaprime.mdbi;

public interface Write<T> {
    BoundWrite<? super T> bind(Context ctxt);

    interface Context {
        <T> Write<? super T> get(Class<T> klass);
    }
}
