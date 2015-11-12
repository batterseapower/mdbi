public class ContextRead<T> implements Read<T> {
    private final Class<T> klass;

    public ContextRead(Class<T> klass) {
        this.klass = klass;
    }

    @Override
    public Class<T> getElementClass() {
        return klass;
    }

    @Override
    public BoundRead<T> bind(Read.Map ctxt) {
        return ctxt.get(klass).bind(ctxt);
    }
}
