package uk.co.omegaprime.mdbi;

class ContextWrite<T> implements Write<T> {
    private final Class<T> klass;

    public ContextWrite(Class<T> klass) {
        this.klass = klass;
    }

    @Override
    public BoundWrite<? super T> bind(Writes.Map ctxt) {
        return ctxt.get(klass).bind(ctxt);
    }
}
