package uk.co.omegaprime.mdbi;

public interface Retry {
    <T extends Throwable> void consider(T e) throws T;
}
