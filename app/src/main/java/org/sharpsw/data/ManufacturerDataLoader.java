package org.sharpsw.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ManufacturerDataLoader implements DataLoader {
    private static final String SELECT_SCRIPT = "SELECT COUNT(1) FROM dbo.FABRICANTE WHERE FABRICANTE_SK = ?";

    public void load(Connection connection, String line) throws SQLException {

    }

    private boolean isInsert(String sk, Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(SELECT_SCRIPT);
        statement.setInt(1, Integer.getInteger(sk));
        ResultSet rs = statement.executeQuery();
        int count = 0;
        if(rs.next()) {
            count = rs.getInt(0);
        }
        rs.close();

        return count == 0;
    }
}
