package uk.co.omegaprime.mdbi;

public interface Read<T> {
    Class<T> getElementClass();

    BoundRead<? extends T> bind(Context ctxt);

    interface Context {
        <T> Read<? extends T> get(Class<T> klass);
    }
}
