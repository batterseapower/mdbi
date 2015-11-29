package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

class MatrixBatchRead implements BatchRead<Object[]> {
    private final Collection<Read<?>> reads;

    public MatrixBatchRead(Collection<Read<?>> reads) {
        this.reads = reads;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object[] get(@Nonnull Read.Context ctxt, @Nonnull ResultSet rs) throws SQLException {
        final List<BoundRead<?>> boundReads = reads.stream().map(read -> read.bind(ctxt)).collect(Collectors.toList());

        final List<?>[] columnLists = new List<?>[boundReads.size()];
        for (int i = 0; i < columnLists.length; i++) {
            columnLists[i] = new ArrayList<>();
        }

        while (rs.next()) {
            final IndexRef ix = IndexRef.create();
            for (int i = 0; i < columnLists.length; i++) {
                ((List<Object>)columnLists[i]).add(boundReads.get(i).get(rs, ix));
            }
        }

        final Object[] columns = new Object[columnLists.length];
        final Iterator<Read<?>> readsIt = reads.iterator();
        for (int i = 0; i < columnLists.length; i++) {
            final List list = columnLists[i];
            columns[i] = Primitives.listToArray(readsIt.next().getElementClass(), list);
        }

        return columns;
    }

}
