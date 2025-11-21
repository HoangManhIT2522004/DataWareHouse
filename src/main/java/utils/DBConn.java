package utils;

import java.sql.*;

public class DBConn {
    private final String url;
    private final String username;
    private final String password;

    public DBConn(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    public void executeQuery(String sql, ResultSetHandler handler, Object... params) throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement stmt = prepare(conn, sql, params);
             ResultSet rs = stmt.executeQuery()) {

            handler.handle(rs); // callback xử lý
        }
    }

    public int executeUpdate(String sql, Object... params) throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement stmt = prepare(conn, sql, params)) {

            return stmt.executeUpdate();
        }
    }

    private PreparedStatement prepare(Connection conn, String sql, Object... params) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(sql);
        for (int i = 0; i < params.length; i++) {
            stmt.setObject(i + 1, params[i]);
        }
        return stmt;
    }

    public interface ResultSetHandler {
        void handle(ResultSet rs) throws SQLException;
    }
}
