public interface Read<T> {
    Class<T> getElementClass();

    BoundRead<T> bind(Reads.Map ctxt);
}
