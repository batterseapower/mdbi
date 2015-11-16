package uk.co.omegaprime.mdbi;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SnocListTest {
    @Test
    public void basicOperation() {
        assertEquals(Arrays.asList(1, 2, 3), iterableToList(SnocList.singleton(1).snoc(2).snoc(3)));
        assertEquals(Arrays.asList(1, 2, 3, 4), iterableToList(SnocList.singleton(1).snocs(SnocList.singleton(2).snoc(3)).snoc(4)));
    }

    private static <T> List<T> iterableToList(Iterable<T> xs) {
        final List<T> result = new ArrayList<>();
        for (T x : xs) {
            result.add(x);
        }

        return result;
    }
}
