package uk.co.omegaprime.mdbi;

public interface Write<T> {
    BoundWrite<? super T> bind(Writes.Map ctxt);
}
