package userpackage;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.co.omegaprime.mdbi.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.omegaprime.mdbi.MDBI.sql;

public class MDBIRetryTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test(expected=SQLException.class)
    public void failIfDeadlockyAndNoRetry() throws SQLException, IOException, ExecutionException, InterruptedException {
        tryDeadlockyTransaction(Retries::nothing);
    }

    @Test
    public void succeedIfDeadlockyAndRetry() throws SQLException, IOException, ExecutionException, InterruptedException {
        tryDeadlockyTransaction(() -> new Retry() {
            @Override
            public <T extends Throwable> void consider(T e) throws T {
                if (e instanceof SQLException && e.getMessage().contains("is locked")) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException _interrupted) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    throw e;
                }
            }
        });
    }

    private void tryDeadlockyTransaction(Supplier<Retry> retryPolicy) throws SQLException, IOException, ExecutionException, InterruptedException {
        final File tempFile = temporaryFolder.newFile();
        final Context ctxt = Context.DEFAULT;

        try (final Connection conn1 = DriverManager.getConnection("jdbc:sqlite:" + tempFile.toString());
             final Connection conn2 = DriverManager.getConnection("jdbc:sqlite:" + tempFile.toString())) {
            final MDBI m1 = MDBI.of(ctxt, conn1);
            // Using unprepared statement here because of a bug in the SQLite library:
            // https://github.com/xerial/sqlite-jdbc/pull/72
            final MDBI m2 = MDBI.of(ctxt, conn2).withPrepared(false);

            m1.execute(sql("create table tab (id int)"));
            m1.execute(sql("insert into tab (id) values (1)"));

            conn1.setAutoCommit(false);
            m1.execute(sql("insert into tab (id) values (2)"));

            final ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                final Future<?> future = executor.submit(() -> {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        try {
                            conn1.setAutoCommit(true);
                        } catch (SQLException e) {
                            throw new UndeclaredThrowableException(e);
                        }
                    }
                });

                // Using "update" rather than the more natural "execute" because of a bug in the SQLite library:
                // https://github.com/xerial/sqlite-jdbc/pull/72
                m2.withRetryPolicy(retryPolicy).update(sql("insert into tab (id) values (3)"));

                // Deliver any background thread exceptions to the main thread
                future.get();
            } finally {
                executor.shutdown();
            }

            assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)),
                         m2.query(sql("select id from tab"), BatchReads.asSet(Reads.useContext(int.class))));
        }
    }
}
