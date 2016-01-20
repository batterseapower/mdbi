package uk.co.omegaprime.mdbi;

import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

public class SQLInterfaceConformanceTest {
    @Test
    public void sqlReturningMethodsHaveStaticEquivalent() throws NoSuchMethodException {
        for (Method m : SQL.class.getDeclaredMethods()) {
            if ((m.getModifiers() & Modifier.STATIC) != 0) {
                continue;
            }

            if ((m.getModifiers() & Modifier.PUBLIC) == 0) {
                continue;
            }

            if (m.getReturnType() != SQL.class) {
                continue;
            }

            // A static version of SQL#sql(SQL) would be pointless!
            if (m.getName().equals("sql") && Arrays.equals(m.getParameterTypes(), new Class[] { SQL.class })) {
                continue;
            }

            MDBI.class.getMethod(m.getName(), m.getParameterTypes());
        }
    }
}
