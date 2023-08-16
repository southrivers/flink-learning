package test.clickhouse;
import com.clickhouse.jdbc.ClickHouseDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TestCk {

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:ch://192.168.28.11:8123/default"; // use http protocol and port 8123 by default
// String url = "jdbc:ch://my-server:8443/system?ssl=true&sslmode=strict&&sslrootcert=/mine.crt";
        Properties properties = new Properties();
// properties.setProperty("ssl", "true");
// properties.setProperty("sslmode", "NONE"); // NONE to trust all servers; STRICT for trusted only
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        try (Connection conn = dataSource.getConnection("default", "");
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from numbers(50000)");
            while(rs.next()) {
                System.out.println(rs.getInt(1));
            }
        }
    }
}
