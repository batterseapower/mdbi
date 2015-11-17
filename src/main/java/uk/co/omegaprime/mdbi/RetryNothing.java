package uk.co.omegaprime.mdbi;

class RetryNothing implements Retry {
    @Override
    public <T extends Throwable> void consider(T e) throws T {
        throw e;
    }
}
