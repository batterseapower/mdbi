package uk.co.omegaprime.mdbi;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

class DropColumnsResultSetMetaData implements ResultSetMetaData {
    private final int firstIx;
    private final ResultSetMetaData rsmd;

    public DropColumnsResultSetMetaData(int firstIx, ResultSetMetaData rsmd) {
        this.firstIx = firstIx;
        this.rsmd = rsmd;
    }

    private int ix(int columnIndex) throws SQLException {
        if (columnIndex < 1) {
            throw new SQLException("Invalid column index " + columnIndex);
        } else {
            return (firstIx - 1) + columnIndex;
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        return rsmd.getColumnCount() - (firstIx - 1);
    }

    // *** Everything below this line is horrible repetitive code that just delegates to rs, possibly via the ix function

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return rsmd.isAutoIncrement(ix(column));
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return rsmd.isCaseSensitive(ix(column));
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return rsmd.isSearchable(ix(column));
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return rsmd.isCurrency(ix(column));
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return rsmd.isNullable(ix(column));
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return rsmd.isSigned(ix(column));
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return rsmd.getColumnDisplaySize(ix(column));
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return rsmd.getColumnLabel(ix(column));
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return rsmd.getColumnName(ix(column));
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return rsmd.getSchemaName(ix(column));
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return rsmd.getPrecision(ix(column));
    }

    @Override
    public int getScale(int column) throws SQLException {
        return rsmd.getScale(ix(column));
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return rsmd.getTableName(ix(column));
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return rsmd.getCatalogName(ix(column));
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return rsmd.getColumnType(ix(column));
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return rsmd.getColumnTypeName(ix(column));
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return rsmd.isReadOnly(ix(column));
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return rsmd.isWritable(ix(column));
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return rsmd.isDefinitelyWritable(ix(column));
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return rsmd.getColumnClassName(ix(column));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return rsmd.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return rsmd.isWrapperFor(iface);
    }
}
