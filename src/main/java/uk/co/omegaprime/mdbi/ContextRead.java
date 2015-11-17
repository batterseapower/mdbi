package uk.co.omegaprime.mdbi;

class ContextRead<T> implements Read<T> {
    private final Class<T> klass;

    public ContextRead(Class<T> klass) {
        this.klass = klass;
    }

    @Override
    public Class<T> getElementClass() {
        return klass;
    }

    @Override
    public BoundRead<? extends T> bind(Read.Context ctxt) {
        return ctxt.get(klass).bind(ctxt);
    }
}
