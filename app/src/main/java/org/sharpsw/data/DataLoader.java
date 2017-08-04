package org.sharpsw.data;


import java.sql.Connection;
import java.sql.SQLException;

public interface DataLoader {

    void load(Connection connection, String line) throws SQLException;
}
