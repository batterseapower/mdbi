public class SQL {
    final Object[] args;

    private SQL(Object[] args) {
        this.args = args;
    }

    public static SQL of(Object... args) {
        return new SQL(args);
    }
}
