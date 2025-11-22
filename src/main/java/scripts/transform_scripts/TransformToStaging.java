package scripts.transform_scripts;

import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;

import java.sql.CallableStatement;
import java.sql.Connection;

public class TransformToStaging {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("   WEATHER ETL - STEP 7: RAW -> STG");
        System.out.println("========================================");

        try {
            // 1. Load Config
            // Lưu ý: Dùng config.xml để lấy thông tin kết nối tới Staging DB
            LoadConfig config = new LoadConfig("config/config.xml");

            // 2. Kết nối Database Staging
            DBConn stagingDB = connectStagingDB(config);

            System.out.println("[Process] Cleaning data from Raw to Staging...");

            // 3. Gọi Stored Procedure
            Connection conn = stagingDB.getConnection();
            // Tên procedure phải khớp với cái bạn vừa tạo trong SQL
            String sql = "{call proc_transform_raw_to_stg()}";

            CallableStatement cstmt = conn.prepareCall(sql);
            cstmt.execute();

            System.out.println("Transform Raw -> Stg COMPLETED successfully!");

            // 4. Cập nhật Log (Nếu bạn có bảng log_process)
            // updateLog(controlDB, ...);

            System.exit(0);

        } catch (Exception e) {
            System.err.println("TRANSFORM FAILED");
            e.printStackTrace();
            // Gửi mail báo lỗi
            EmailSender.sendError("ETL Error: Transform Raw->Stg Failed", e.getMessage(), e);
            System.exit(1);
        }
    }

    // Hàm phụ để kết nối Staging DB
    private static DBConn connectStagingDB(LoadConfig config) throws Exception {
        var database = LoadConfig.getElement(config.getXmlDoc(), "database");
        var staging = LoadConfig.getChildElement(database, "staging");

        String url = LoadConfig.getValue(staging, "url");
        String user = LoadConfig.getValue(staging, "username");
        String pass = LoadConfig.getValue(staging, "password");

        return new DBConn(url, user, pass);
    }
}