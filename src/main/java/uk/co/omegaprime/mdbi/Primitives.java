package uk.co.omegaprime.mdbi;

import java.lang.reflect.Array;
import java.util.List;

class Primitives {
    private Primitives() {}

    static Object listToArray(Class<?> klass, List<?> list) {
        if (klass == boolean.class) {
            final boolean[] result = new boolean[list.size()];
            for (int i = 0; i < list.size(); i++) result[i] = (boolean)(Boolean)list.get(i);
            return result;
        } else if (klass == byte.class) {
            final byte[] result = new byte[list.size()];
            for (int i = 0; i < list.size(); i++) result[i] = (byte)(Byte)list.get(i);
            return result;
        } else if (klass == char.class) {
            final char[] result = new char[list.size()];
            for (int i = 0; i < list.size(); i++) result[i] = (char)(Character)list.get(i);
            return result;
        } else if (klass == short.class) {
            final short[] result = new short[list.size()];
            for (int i = 0; i < list.size(); i++) result[i] = (short)(Short)list.get(i);
            return result;
        } else if (klass == int.class) {
            final int[] result = new int[list.size()];
            for (int i = 0; i < list.size(); i++) result[i] = (int)(Integer)list.get(i);
            return result;
        } else if (klass == long.class) {
            final long[] result = new long[list.size()];
            for (int i = 0; i < list.size(); i++) result[i] = (long)(Long)list.get(i);
            return result;
        } else if (klass == float.class) {
            final float[] result = new float[list.size()];
            for (int i = 0; i < list.size(); i++) result[i] = (float)(Float)list.get(i);
            return result;
        } else if (klass == double.class) {
            final double[] result = new double[list.size()];
            for (int i = 0; i < list.size(); i++) result[i] = (double)(Double)list.get(i);
            return result;
        } else if (Object.class.isAssignableFrom(klass)) {
            final Object[] result = (Object[]) Array.newInstance(klass, list.size());
            for (int i = 0; i < list.size(); i++) result[i] = list.get(i);
            return result;
        } else {
            throw new IllegalStateException("Please add support for primitive type " + klass);
        }
    }
}
