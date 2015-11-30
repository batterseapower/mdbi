package uk.co.omegaprime.mdbi;

import java.sql.ResultSet;
import java.sql.SQLException;

// A ResultSet that initially pretends to be on row N-1, when the underlying ResultSet is actually on row N.
// NB: the supplied ResultSet is assumed to be on a valid row initially.
class PeekedResultSet
        // NB: just delegating all the update/get methods is not strictly right, because if someone tries to
        // use them before we have used the peeked row, we would need to forceUsePeekedRow() first.
        // This doesn't matter for my application (because we ask nicely in the Read docs not to do this :-)
        // so I conveniently forget about it.
        extends DelegatingResultSet {
    private boolean usedPeekedRow = false;

    public PeekedResultSet(ResultSet rs) {
        super(rs);
    }

    public boolean isUsedPeekedRow() {
        return usedPeekedRow;
    }

    @Override
    public boolean next() throws SQLException {
        if (!usedPeekedRow) {
            usedPeekedRow = true;
            return true;
        } else {
            return rs.next();
        }
    }

    // Forces usedPeekedRow to true. We only want to do this at the last possible
    // moment when there is no alternative, because moving back in the ResultSet
    // might not be supported (forward-only ResultSets are the common case).
    //
    // If returns false, we are certainly not on a valid row. If returns true, we might be.
    private boolean forceUsePeekedRow() throws SQLException {
        if (!usedPeekedRow) {
            usedPeekedRow = true;
            if (!rs.previous()) return false;
        }

        return true;
    }

    @Override
    public boolean previous() throws SQLException {
        return forceUsePeekedRow() && rs.previous();

    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return usedPeekedRow ? rs.isBeforeFirst() : rs.isFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return usedPeekedRow && rs.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return forceUsePeekedRow() && rs.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return usedPeekedRow && rs.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        usedPeekedRow = true;
        rs.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        usedPeekedRow = true;
        rs.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        usedPeekedRow = true;
        return rs.first();
    }

    @Override
    public boolean last() throws SQLException {
        usedPeekedRow = true;
        return rs.last();
    }

    @Override
    public int getRow() throws SQLException {
        return usedPeekedRow ? rs.getRow() : rs.getRow() - 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        usedPeekedRow = true;
        return rs.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (usedPeekedRow) {
            return rs.relative(rows);
        } else {
            usedPeekedRow = true;
            if (rows == 0) {
                return relative(0);
            } else if (rows > 0) {
                return relative(rows - 1);
            } else {
                return relative(rows + 1);
            }
        }
    }
}