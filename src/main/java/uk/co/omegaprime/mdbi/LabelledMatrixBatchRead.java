package uk.co.omegaprime.mdbi;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

class LabelledMatrixBatchRead implements BatchRead<Map<String, Object>> {
    private final Collection<Read<?>> reads;

    public LabelledMatrixBatchRead(Collection<Read<?>> reads) {
        this.reads = reads;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> get(Read.Context ctxt, ResultSet rs) throws SQLException {
        final List<BoundRead<?>> boundReads = reads.stream().map(read -> read.bind(ctxt)).collect(Collectors.toList());

        final List<?>[] columnLists = new List<?>[reads.size()];
        for (int i = 0; i < columnLists.length; i++) {
            columnLists[i] = new ArrayList<>();
        }

        final String[] keys = new String[reads.size()];
        {
            final ResultSetMetaData rsmd = rs.getMetaData();

            int ix = 1;
            int i = 0;
            for (BoundRead<?> boundRead : boundReads) {
                keys[i++] = rsmd.getColumnName(ix);
                ix += boundRead.arity();
            }
        }

        while (rs.next()) {
            final IndexRef ix = IndexRef.create();
            for (int i = 0; i < columnLists.length; i++) {
                ((List<Object>)columnLists[i]).add(boundReads.get(i).get(rs, ix));
            }
        }

        final Map<String, Object> result = new LinkedHashMap<>();
        final Iterator<Read<?>> readsIt = reads.iterator();
        for (int i = 0; i < columnLists.length; i++) {
            if (result.containsKey(keys[i])) {
                throw new IllegalArgumentException("Column " + keys[i] + " occurs in ResultSet twice");
            }

            final List list = columnLists[i];
            result.put(keys[i], Primitives.listToArray(readsIt.next().getElementClass(), list));
        }

        return result;
    }
}
