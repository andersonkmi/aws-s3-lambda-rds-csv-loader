package org.sharpsw.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnectionBuilder {
    public Connection getConnection(String server, String user, String password, String dbname) throws SQLException {
        StringBuilder buffer = new StringBuilder();
        buffer.append("jdbc:jtds:sqlserver://").append(server).append("/").append(dbname);
        Connection connection = DriverManager.getConnection(buffer.toString(), user, password);
        return connection;
    }
}
