package uk.co.omegaprime.mdbi;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Time;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class DropColumnsResultSet implements ResultSet {
    private final ResultSet rs;
    private final int firstIx;

    // Lazy-initialized
    private Set<String> validColumnLabels = null;

    public DropColumnsResultSet(int firstIx, ResultSet rs) {
        if (firstIx < 1) {
            throw new IllegalArgumentException("The first column index must be at least 1, not " + firstIx);
        }

        this.firstIx = firstIx;
        this.rs = rs;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new DropColumnsResultSetMetaData(firstIx, rs.getMetaData());
    }

    private int ix(int columnIndex) throws SQLException {
        if (columnIndex < 1) {
            throw new SQLException("Invalid column index " + columnIndex);
        } else {
            return (firstIx - 1) + columnIndex;
        }
    }

    private String label(String columnLabel) throws SQLException {
        if (validColumnLabels == null) {
            final ResultSetMetaData rsmd = rs.getMetaData();
            validColumnLabels = new HashSet<>();
            for (int i = firstIx; i <= rsmd.getColumnCount(); i++) {
                validColumnLabels.add(rsmd.getColumnLabel(i));
            }
        }

        if (validColumnLabels.contains(columnLabel)) {
            return columnLabel;
        } else {
            throw new SQLException("Invalid column label " + columnLabel);
        }
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        final int ix = rs.findColumn(columnLabel);
        if (ix < firstIx) {
            throw new SQLException("Invalid column label " + columnLabel);
        } else {
            return ix - (firstIx - 1);
        }
    }

    // *** Everything below this line is horrible repetitive code that just delegates to rs, possibly via the label/ix functions if applicable

    @Override
    public boolean next() throws SQLException {
        return rs.next();
    }

    @Override
    public void close() throws SQLException {
        rs.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return rs.wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return rs.getString(ix(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return rs.getBoolean(ix(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return rs.getByte(ix(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return rs.getShort(ix(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return rs.getInt(ix(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return rs.getLong(ix(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return rs.getFloat(ix(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return rs.getDouble(ix(columnIndex));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return rs.getBigDecimal(ix(columnIndex), scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return rs.getBytes(ix(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return rs.getDate(ix(columnIndex));
    }

    @Override
    public java.sql.Time getTime(int columnIndex) throws SQLException {
        return rs.getTime(ix(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return rs.getTimestamp(ix(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return rs.getAsciiStream(ix(columnIndex));
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return rs.getUnicodeStream(ix(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return rs.getBinaryStream(ix(columnIndex));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return rs.getString(label(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return rs.getBoolean(label(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return rs.getByte(label(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return rs.getShort(label(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return rs.getInt(label(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return rs.getLong(label(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return rs.getFloat(label(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return rs.getDouble(label(columnLabel));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return rs.getBigDecimal(label(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return rs.getBytes(label(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return rs.getDate(label(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return rs.getTime(label(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return rs.getTimestamp(label(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return rs.getAsciiStream(label(columnLabel));
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return rs.getUnicodeStream(label(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return rs.getBinaryStream(label(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return rs.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        rs.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return rs.getCursorName();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return rs.getObject(ix(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return rs.getObject(label(columnLabel));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return rs.getCharacterStream(ix(columnIndex));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return rs.getCharacterStream(label(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return rs.getBigDecimal(ix(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return rs.getBigDecimal(label(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return rs.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return rs.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return rs.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return rs.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        rs.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        rs.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        return rs.first();
    }

    @Override
    public boolean last() throws SQLException {
        return rs.last();
    }

    @Override
    public int getRow() throws SQLException {
        return rs.getRow();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return rs.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return rs.relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return rs.previous();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        rs.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return rs.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        rs.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return rs.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return rs.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return rs.getConcurrency();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return rs.rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return rs.rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return rs.rowDeleted();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        rs.updateNull(ix(columnIndex));
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        rs.updateBoolean(ix(columnIndex), x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        rs.updateByte(ix(columnIndex), x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        rs.updateShort(ix(columnIndex), x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        rs.updateInt(ix(columnIndex), x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        rs.updateLong(ix(columnIndex), x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        rs.updateFloat(ix(columnIndex), x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        rs.updateDouble(ix(columnIndex), x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        rs.updateBigDecimal(ix(columnIndex), x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        rs.updateString(ix(columnIndex), x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        rs.updateBytes(ix(columnIndex), x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        rs.updateDate(ix(columnIndex), x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        rs.updateTime(ix(columnIndex), x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        rs.updateTimestamp(ix(columnIndex), x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        rs.updateAsciiStream(ix(columnIndex), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        rs.updateBinaryStream(ix(columnIndex), x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        rs.updateCharacterStream(ix(columnIndex), x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        rs.updateObject(ix(columnIndex), x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        rs.updateObject(ix(columnIndex), x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        rs.updateNull(label(columnLabel));
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        rs.updateBoolean(label(columnLabel), x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        rs.updateByte(label(columnLabel), x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        rs.updateShort(label(columnLabel), x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        rs.updateInt(label(columnLabel), x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        rs.updateLong(label(columnLabel), x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        rs.updateFloat(label(columnLabel), x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        rs.updateDouble(label(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        rs.updateBigDecimal(label(columnLabel), x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        rs.updateString(label(columnLabel), x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        rs.updateBytes(label(columnLabel), x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        rs.updateDate(label(columnLabel), x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        rs.updateTime(label(columnLabel), x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        rs.updateTimestamp(label(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        rs.updateAsciiStream(label(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        rs.updateBinaryStream(label(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        rs.updateCharacterStream(label(columnLabel), reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        rs.updateObject(label(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        rs.updateObject(label(columnLabel), x);
    }

    @Override
    public void insertRow() throws SQLException {
        rs.insertRow();
    }

    @Override
    public void updateRow() throws SQLException {
        rs.updateRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        rs.deleteRow();
    }

    @Override
    public void refreshRow() throws SQLException {
        rs.refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        rs.cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        rs.moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        rs.moveToCurrentRow();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return rs.getStatement();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return rs.getObject(ix(columnIndex), map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return rs.getRef(ix(columnIndex));
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return rs.getBlob(ix(columnIndex));
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return rs.getClob(ix(columnIndex));
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return rs.getArray(ix(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return rs.getObject(label(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return rs.getRef(label(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return rs.getBlob(label(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return rs.getClob(label(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return rs.getArray(label(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return rs.getDate(ix(columnIndex), cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return rs.getDate(label(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return rs.getTime(ix(columnIndex), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return rs.getTime(label(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return rs.getTimestamp(ix(columnIndex), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return rs.getTimestamp(label(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return rs.getURL(ix(columnIndex));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return rs.getURL(label(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        rs.updateRef(ix(columnIndex), x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        rs.updateRef(label(columnLabel), x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        rs.updateBlob(ix(columnIndex), x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        rs.updateBlob(label(columnLabel), x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        rs.updateClob(ix(columnIndex), x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        rs.updateClob(label(columnLabel), x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        rs.updateArray(ix(columnIndex), x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        rs.updateArray(label(columnLabel), x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return rs.getRowId(ix(columnIndex));
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return rs.getRowId(label(columnLabel));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        rs.updateRowId(ix(columnIndex), x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        rs.updateRowId(label(columnLabel), x);
    }

    @Override
    public int getHoldability() throws SQLException {
        return rs.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return rs.isClosed();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        rs.updateNString(ix(columnIndex), nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        rs.updateNString(label(columnLabel), nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        rs.updateNClob(ix(columnIndex), nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        rs.updateNClob(label(columnLabel), nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return rs.getNClob(ix(columnIndex));
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return rs.getNClob(label(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return rs.getSQLXML(ix(columnIndex));
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return rs.getSQLXML(label(columnLabel));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        rs.updateSQLXML(ix(columnIndex), xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        rs.updateSQLXML(label(columnLabel), xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return rs.getNString(ix(columnIndex));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return rs.getNString(label(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return rs.getNCharacterStream(ix(columnIndex));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return rs.getNCharacterStream(label(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        rs.updateNCharacterStream(ix(columnIndex), x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        rs.updateNCharacterStream(label(columnLabel), reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        rs.updateAsciiStream(ix(columnIndex), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        rs.updateBinaryStream(ix(columnIndex), x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        rs.updateCharacterStream(ix(columnIndex), x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        rs.updateAsciiStream(label(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        rs.updateBinaryStream(label(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        rs.updateCharacterStream(label(columnLabel), reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        rs.updateBlob(ix(columnIndex), inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        rs.updateBlob(label(columnLabel), inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        rs.updateClob(ix(columnIndex), reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        rs.updateClob(label(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        rs.updateNClob(ix(columnIndex), reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        rs.updateNClob(label(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        rs.updateNCharacterStream(ix(columnIndex), x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        rs.updateNCharacterStream(label(columnLabel), reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        rs.updateAsciiStream(ix(columnIndex), x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        rs.updateBinaryStream(ix(columnIndex), x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        rs.updateCharacterStream(ix(columnIndex), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        rs.updateAsciiStream(label(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        rs.updateBinaryStream(label(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        rs.updateCharacterStream(label(columnLabel), reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        rs.updateBlob(ix(columnIndex), inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        rs.updateBlob(label(columnLabel), inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        rs.updateClob(ix(columnIndex), reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        rs.updateClob(label(columnLabel), reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        rs.updateNClob(ix(columnIndex), reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        rs.updateNClob(label(columnLabel), reader);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return rs.getObject(ix(columnIndex), type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return rs.getObject(label(columnLabel), type);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        rs.updateObject(ix(columnIndex), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        rs.updateObject(label(columnLabel), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        rs.updateObject(ix(columnIndex), x, targetSqlType);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        rs.updateObject(label(columnLabel), x, targetSqlType);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return rs.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return rs.isWrapperFor(iface);
    }
}
