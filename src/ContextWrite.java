public class ContextWrite<T> implements Write<T> {
    private final Class<T> klass;

    public ContextWrite(Class<T> klass) {
        this.klass = klass;
    }

    @Override
    public BoundWrite<T> bind(Map ctxt) {
        return ctxt.get(klass).bind(ctxt);
    }
}
