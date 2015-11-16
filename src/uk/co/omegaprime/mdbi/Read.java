package uk.co.omegaprime.mdbi;

public interface Read<T> {
    Class<T> getElementClass();

    BoundRead<? extends T> bind(Reads.Map ctxt);
}
