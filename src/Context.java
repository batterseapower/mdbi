import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

public class Context {
    final Read.Map readers = new Read.Map();
    final Write.Map writers = new Write.Map();

    public static Context createDefault() {
        final Context context = new Context();
        context.register(int.class, Write.INT, Read.INT);
        context.register(Integer.class, Write.INTEGER, Read.INTEGER);
        context.register(String.class, Write.STRING, Read.STRING);
        context.register(LocalDate.class, Write.LOCAL_DATE, Read.LOCAL_DATE);
        context.register(LocalDateTime.class, Write.LOCAL_DATE_TIME, Read.LOCAL_DATE_TIME);

        // FIXME: register more default stuff
        return context;
    }

    public <T> void register(Class<T> klass, Write<T> write, Read<T> read) {
        readers.put(klass, read);
        writers.put(klass, write);
    }

    public <T> void registerRead(Class<? super T> klass, Read<T> read) {
        readers.put(klass, read);
    }

    public <T> void registerWrite(Class<? extends T> klass, Write<T> write) {
        writers.put(klass, write);
    }
}
