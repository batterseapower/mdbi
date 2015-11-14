package uk.co.omegaprime.mdbi;

public interface Write<T> {
    BoundWrite<T> bind(Writes.Map ctxt);
}
