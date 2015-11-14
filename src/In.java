public class In {
    final Object[] args;

    private In(Object[] args) {
        this.args = args;
    }

    public static In of(Object... args) {
        return new In(args);
    }
}
