package uk.co.omegaprime.mdbi;

import java.sql.ResultSet;
import java.sql.SQLException;

class ContiguouslyFilteredResultSet extends DelegatingResultSet {
    public interface ShouldContinue {
        boolean ok() throws SQLException;
    }

    // NB: this predicate will in general make use of "rs", so make sure that's in the right state before calling this
    private final ShouldContinue rowMatches;

    private int row = 0;

    // 0: not started
    // 1: started
    // 2: finished
    private byte state = 0;

    public ContiguouslyFilteredResultSet(ResultSet rs, ShouldContinue rowMatches) {
        super(rs);
        this.rowMatches = rowMatches;
    }

    private boolean rowOK() throws SQLException {
        // Alas, it is *not* sufficient to just use rowMatches to delimit the valid region
        // of the ResultSet. Reason: in some pathological cases we might have some rows *before*
        // our initial row which match the predicate, and we don't want to include those. This
        // is the purpose of this row index, which can otherwise get away without.
        return row > 0 && rowMatches.ok();
    }

    @Override
    public boolean next() throws SQLException {
        if (state == 2) return false;

        row++;
        if (!rs.next() || !rowOK()) {
            state = 2;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public boolean previous() throws SQLException {
        if (state == 0) return false;

        row--;
        if (!rs.previous() || !rowOK()) {
            state = 0;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return state == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return state == 2;
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (state == 1) {
            try {
                return !previous();
            } finally {
                if (!next()) {
                    throw new SQLException("Failed to move back to original position after isFirst() test");
                }
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean isLast() throws SQLException {
        if (state == 1) {
            try {
                return !next();
            } finally {
                if (!previous())  {
                    throw new SQLException("Failed to move back to original position after isLast() test");
                }
            }
        } else {
            return false;
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public void beforeFirst() throws SQLException {
        while (previous());
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public void afterLast() throws SQLException {
        while (next());
    }

    @Override
    public boolean first() throws SQLException {
        beforeFirst();
        return next();
    }

    @Override
    public boolean last() throws SQLException {
        afterLast();
        return previous();
    }

    @Override
    public int getRow() throws SQLException {
        if (state != 1) throw new SQLException("ResultSet.getRow() on ResultSet with no current row");

        return row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (row == 0) {
            beforeFirst();
            return false;
        } else if (row > 0) {
            // If target row is row+1 we need to go forward 1. If target row is row-1 we need to go backward 1.
            return relative(row - this.row);
        } else {
            // If row is -1 then we need to skip backward 0 additional rows after the first
            return last() && relative(row + 1);
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (rows == 0) {
            return state == 1;
        } else if (rows > 0) {
            for (int i = 0; i < rows; i++) {
                if (!next()) return false;
            }
            return true;
        } else {
            for (int i = 0; i < -rows; i++) {
                if (!previous()) return false;
            }
            return true;
        }
    }
}
