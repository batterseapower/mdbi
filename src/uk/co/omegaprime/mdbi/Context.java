package uk.co.omegaprime.mdbi;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class Context {
    final Reads.Map readers = new Reads.Map();
    final Writes.Map writers = new Writes.Map();

    public static Context createDefault() {
        final Context context = new Context();
        context.register(boolean.class,       Writes.PRIM_BOOLEAN,    Reads.PRIM_BOOLEAN);
        context.register(Boolean.class,       Writes.BOOLEAN,         Reads.BOOLEAN);
        context.register(byte.class,          Writes.PRIM_BYTE,       Reads.PRIM_BYTE);
        context.register(Byte.class,          Writes.BYTE,            Reads.BYTE);
        context.register(char.class,          Writes.PRIM_CHAR,       Reads.PRIM_CHAR);
        context.register(Character.class,     Writes.CHARACTER,       Reads.CHARACTER);
        context.register(short.class,         Writes.PRIM_SHORT,      Reads.PRIM_SHORT);
        context.register(Short.class,         Writes.SHORT,           Reads.SHORT);
        context.register(int.class,           Writes.PRIM_INT,        Reads.PRIM_INT);
        context.register(Integer.class,       Writes.INTEGER,         Reads.INTEGER);
        context.register(long.class,          Writes.PRIM_LONG,       Reads.PRIM_LONG);
        context.register(Long.class,          Writes.LONG,            Reads.LONG);
        context.register(float.class,         Writes.PRIM_FLOAT,      Reads.PRIM_FLOAT);
        context.register(Float.class,         Writes.FLOAT,           Reads.FLOAT);
        context.register(double.class,        Writes.PRIM_DOUBLE,     Reads.PRIM_DOUBLE);
        context.register(Double.class,        Writes.DOUBLE,          Reads.DOUBLE);
        context.register(String.class,        Writes.STRING,          Reads.STRING);
        context.register(LocalDate.class,     Writes.LOCAL_DATE,      Reads.LOCAL_DATE);
        context.register(LocalTime.class,     Writes.LOCAL_TIME,      Reads.LOCAL_TIME);
        context.register(LocalDateTime.class, Writes.LOCAL_DATE_TIME, Reads.LOCAL_DATE_TIME);
        context.register(byte[].class,        Writes.BYTE_ARRAY,      Reads.BYTE_ARRAY);

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
