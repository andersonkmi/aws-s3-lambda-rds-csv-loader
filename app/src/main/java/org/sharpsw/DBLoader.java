package org.sharpsw;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBLoader {
    private Connection connection;
    private String server;
    private String user;
    private String password;
    private String dbname;
    private LambdaLogger logger;

    public DBLoader(String server, String user, String password, String dbname, LambdaLogger logger) {
        this.server = server;
        this.user = user;
        this.password = password;
        this.dbname = dbname;
        this.logger = logger;
    }

    public void load(String line) {
        //createConnection();
    }

    public void createConnection() {
        if(connection == null) {
            StringBuilder buffer = new StringBuilder();
            buffer.append("jdbc:jtds:sqlserver://").append(server).append("/").append(dbname);
            try {
                connection = DriverManager.getConnection(buffer.toString(), user, password);
                logger.log("DB conn ok");
            } catch (SQLException exception) {
                logger.log(exception.getMessage());
            }
        }
    }

    public void closeConnection() {
        if(connection != null) {
            try {
                connection.close();
                logger.log("DB conn closed");
            } catch (SQLException exception) {
                logger.log(exception.getMessage());
            }
        }
    }
}
