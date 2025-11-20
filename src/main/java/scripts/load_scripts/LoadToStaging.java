package scripts.load_scripts;

import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;
import org.w3c.dom.Element;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LoadToStaging {

    private static DBConn controlDB;
    private static DBConn stagingDB;

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("Weather ETL - Load to Staging Started");
        System.out.println("========================================\n");

        // Biến để lưu ID dùng cho xử lý lỗi
        final String[] currentExecutionIdWrapper = {null};

        try {
            // 1. Load Configuration
            LoadConfig config = new LoadConfig("D:\\DataWareHouse\\src\\main\\java\\config\\config.xml");

            // 2. Kết nối Database
            controlDB = connectControlDB(config);
            stagingDB = connectStagingDB(config);

            // 3. Tìm bản ghi Log có trạng thái 'success' (từ bước Extract)
            // Lưu ý: Code tìm 'success' để khớp với dữ liệu hiện tại của bạn
            String sqlGetLog = "SELECT l.execution_id, cs.output_path " +
                    "FROM log_src l " +
                    "JOIN config_src cs ON l.config_src_id = cs.config_src_id " +
                    "WHERE l.status = 'success'::src_status " +
                    "ORDER BY l.execution_id DESC LIMIT 1";

            final String[] executionId = {null};
            final String[] outputPath = {null};

            controlDB.executeQuery(sqlGetLog, rs -> {
                if (rs.next()) {
                    // FIX: Lấy ID dưới dạng String
                    executionId[0] = rs.getString("execution_id");
                    outputPath[0] = rs.getString("output_path");
                }
            });

            if (executionId[0] == null) {
                System.out.println("[Step 3] Không tìm thấy log nào (status='success') để load.");
                System.exit(0);
            }

            currentExecutionIdWrapper[0] = executionId[0];
            System.out.println("[Step 3] Found Execution ID: " + executionId[0]);

            // 4. Cập nhật trạng thái đang chạy (Try-catch để tránh lỗi nếu thiếu ENUM 'loading')
            try {
                updateLogStatus(executionId[0], "loading", "Start loading to staging");
            } catch (Exception e) {
                System.out.println("[Log] Warning: Không thể update status 'loading' (có thể do DB thiếu enum). Tiếp tục chạy...");
            }

            // 5. Xác định file CSV cần load
            String dateStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            String fileName = "weatherapi_" + dateStr + ".csv";

            String cleanPath = outputPath[0].replace("\\", "/");
            if (!cleanPath.endsWith("/")) cleanPath += "/";
            String fullFilePath = cleanPath + fileName;

            File csvFile = new File(fullFilePath);
            if (!csvFile.exists()) {
                throw new Exception("File CSV không tồn tại tại: " + fullFilePath);
            }
            System.out.println("  - File Source: " + fullFilePath);

            // 6. Load dữ liệu vào Staging (Tự động tạo bảng nếu chưa có)
            int rowsLoaded = loadDataToStagingTable(fullFilePath, executionId[0]);

            // 7. Di chuyển file vào thư mục archive
            archiveFile(csvFile);

            // 8. Cập nhật trạng thái thành công
            // Dùng 'success' thay vì 'loaded' để an toàn với DB hiện tại của bạn
            updateLogStatus(executionId[0], "success", "Finished Loading: " + rowsLoaded + " records");

            // 9. Gửi email báo cáo
            String subject = "✓ Weather ETL - Load Staging Success";
            String body = "Execution ID: " + executionId[0] + "\n" +
                    "Rows Loaded: " + rowsLoaded + "\n" +
                    "Status: SUCCESS";
            EmailSender.sendEmail(subject, body);

            System.out.println("\n========================================");
            System.out.println("Weather ETL - Load to Staging COMPLETED");
            System.out.println("========================================");

        } catch (Exception e) {
            System.err.println("✗ ERROR: " + e.getMessage());

            // Update status failed nếu có lỗi
            if (currentExecutionIdWrapper[0] != null) {
                try {
                    String errSql = String.format(
                            "UPDATE log_src SET status='failed'::src_status, end_time=NOW(), error_message='%s' WHERE execution_id='%s'",
                            e.getMessage().replace("'", "''"), currentExecutionIdWrapper[0]
                    );
                    controlDB.executeUpdate(errSql);
                } catch (Exception ex) { ex.printStackTrace(); }
            }

            EmailSender.sendError("✗ Weather ETL - Load Staging Failed", e.getMessage(), e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Hàm load dữ liệu chính: Tự động tạo bảng -> Truncate -> Insert
     */
    private static int loadDataToStagingTable(String filePath, String executionId) throws Exception {
        System.out.println("[Step 6] Loading data to Staging DB...");

        Connection conn = stagingDB.getConnection();
        conn.setAutoCommit(false); // Bắt đầu transaction

        int count = 0;
        try {
            // 6.1 TỰ ĐỘNG TẠO BẢNG NẾU CHƯA CÓ (Auto-Create)
            try (java.sql.Statement stmt = conn.createStatement()) {
                String createTableSQL =
                        "CREATE TABLE IF NOT EXISTS weather_staging (" +
                                "    id SERIAL PRIMARY KEY," +
                                "    execution_id VARCHAR(50)," + // VARCHAR để chứa ID dạng chuỗi
                                "    location_name VARCHAR(100)," +
                                "    location_code VARCHAR(50)," +
                                "    region VARCHAR(50)," +
                                "    temp_c DECIMAL(5,2)," +
                                "    temp_f DECIMAL(5,2)," +
                                "    feels_like_c DECIMAL(5,2)," +
                                "    feels_like_f DECIMAL(5,2)," +
                                "    humidity INT," +
                                "    wind_kph DECIMAL(5,2)," +
                                "    wind_mph DECIMAL(5,2)," +
                                "    wind_degree INT," +
                                "    wind_dir VARCHAR(10)," +
                                "    pressure_mb DECIMAL(6,2)," +
                                "    pressure_in DECIMAL(6,2)," +
                                "    precip_mm DECIMAL(5,2)," +
                                "    precip_in DECIMAL(5,2)," +
                                "    cloud INT," +
                                "    uv DECIMAL(4,1)," +
                                "    vis_km DECIMAL(5,1)," +
                                "    vis_miles DECIMAL(5,1)," +
                                "    condition_text VARCHAR(255)," +
                                "    condition_code INT," +
                                "    aqi_us INT," +
                                "    aqi_gb INT," +
                                "    pm2_5 DECIMAL(6,2)," +
                                "    pm10 DECIMAL(6,2)," +
                                "    co DECIMAL(6,2)," +
                                "    no2 DECIMAL(6,2)," +
                                "    o3 DECIMAL(6,2)," +
                                "    so2 DECIMAL(6,2)," +
                                "    last_updated TIMESTAMP," +
                                "    extract_time TIMESTAMP," +
                                "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                                ")";
                stmt.execute(createTableSQL);
                System.out.println("  - Checked/Created table 'weather_staging'.");
            }

            // 6.2 Truncate dữ liệu cũ
            System.out.println("  - Truncating old data...");
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("TRUNCATE TABLE weather_staging RESTART IDENTITY");
            }

            // 6.3 Insert dữ liệu mới từ CSV
            String insertSql = "INSERT INTO weather_staging (" +
                    "execution_id, location_name, location_code, region, " +
                    "temp_c, temp_f, feels_like_c, feels_like_f, " +
                    "humidity, wind_kph, wind_mph, wind_degree, wind_dir, " +
                    "pressure_mb, pressure_in, precip_mm, precip_in, " +
                    "cloud, uv, vis_km, vis_miles, " +
                    "condition_text, condition_code, " +
                    "aqi_us, aqi_gb, pm2_5, pm10, co, no2, o3, so2, " +
                    "last_updated, extract_time" +
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            try (PreparedStatement ps = conn.prepareStatement(insertSql);
                 BufferedReader br = new BufferedReader(new FileReader(filePath))) {

                String line;
                br.readLine(); // Bỏ qua dòng Header

                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;

                    // Tách CSV, xử lý dấu phẩy trong ngoặc kép
                    String[] data = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

                    if (data.length < 33) continue; // Bỏ qua dòng lỗi

                    // Set Params
                    ps.setString(1, executionId); // ID dạng String
                    ps.setString(2, cleanCsvValue(data[1]));
                    ps.setString(3, data[2]);
                    ps.setString(4, data[3]);
                    ps.setDouble(5, parseDoubleSafe(data[4]));
                    ps.setDouble(6, parseDoubleSafe(data[5]));
                    ps.setDouble(7, parseDoubleSafe(data[6]));
                    ps.setDouble(8, parseDoubleSafe(data[7]));
                    ps.setInt(9, parseIntSafe(data[8]));
                    ps.setDouble(10, parseDoubleSafe(data[9]));
                    ps.setDouble(11, parseDoubleSafe(data[10]));
                    ps.setInt(12, parseIntSafe(data[11]));
                    ps.setString(13, cleanCsvValue(data[12]));
                    ps.setDouble(14, parseDoubleSafe(data[13]));
                    ps.setDouble(15, parseDoubleSafe(data[14]));
                    ps.setDouble(16, parseDoubleSafe(data[15]));
                    ps.setDouble(17, parseDoubleSafe(data[16]));
                    ps.setInt(18, parseIntSafe(data[17]));
                    ps.setDouble(19, parseDoubleSafe(data[18]));
                    ps.setDouble(20, parseDoubleSafe(data[19]));
                    ps.setDouble(21, parseDoubleSafe(data[20]));
                    ps.setString(22, cleanCsvValue(data[21]));
                    ps.setInt(23, parseIntSafe(data[22]));
                    ps.setInt(24, parseIntSafe(data[23]));
                    ps.setInt(25, parseIntSafe(data[24]));
                    ps.setDouble(26, parseDoubleSafe(data[25]));
                    ps.setDouble(27, parseDoubleSafe(data[26]));
                    ps.setDouble(28, parseDoubleSafe(data[27]));
                    ps.setDouble(29, parseDoubleSafe(data[28]));
                    ps.setDouble(30, parseDoubleSafe(data[29]));
                    ps.setDouble(31, parseDoubleSafe(data[30]));
                    ps.setTimestamp(32, parseTimestamp(data[31]));
                    ps.setTimestamp(33, parseTimestamp(data[32]));

                    ps.addBatch();
                    count++;

                    if (count % 1000 == 0) ps.executeBatch();
                }
                ps.executeBatch(); // Chạy mẻ cuối cùng
                conn.commit(); // Xác nhận lưu vào DB
                System.out.println("  ✓ Inserted " + count + " records.");
            }

        } catch (Exception e) {
            conn.rollback(); // Hoàn tác nếu có lỗi
            throw e;
        } finally {
            conn.setAutoCommit(true);
            conn.close();
        }
        return count;
    }

    // === CÁC HÀM HỖ TRỢ (HELPERS) ===

    private static void updateLogStatus(String executionId, String status, String message) throws Exception {
        String sql = String.format(
                "UPDATE log_src SET status='%s'::src_status, end_time=NOW(), error_message='%s' WHERE execution_id='%s'",
                status, message.replace("'", "''"), executionId
        );
        controlDB.executeUpdate(sql);
        System.out.println("[Log] Updated status to: " + status);
    }

    private static String cleanCsvValue(String value) {
        if (value != null && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static Double parseDoubleSafe(String val) { try { return Double.parseDouble(val); } catch (Exception e) { return 0.0; } }
    private static Integer parseIntSafe(String val) { try { return Integer.parseInt(val); } catch (Exception e) { return 0; } }

    private static Timestamp parseTimestamp(String dateStr) {
        try {
            if (dateStr.length() == 16) dateStr += ":00"; // Fix thiếu giây
            return Timestamp.valueOf(dateStr);
        } catch (Exception e) {
            return Timestamp.valueOf(LocalDateTime.now());
        }
    }

    private static void archiveFile(File sourceFile) {
        try {
            Path archiveDir = Paths.get(sourceFile.getParent(), "archive");
            if (!Files.exists(archiveDir)) {
                Files.createDirectories(archiveDir);
            }
            Files.move(sourceFile.toPath(), archiveDir.resolve(sourceFile.getName()), StandardCopyOption.REPLACE_EXISTING);
            System.out.println("[Archive] File moved to archive.");
        } catch (Exception e) {
            System.err.println("Warning: Failed to archive file: " + e.getMessage());
        }
    }

    private static DBConn connectControlDB(LoadConfig config) throws Exception {
        Element control = LoadConfig.getElement(config.getXmlDoc(), "control");
        if (control == null) control = LoadConfig.getChildElement(LoadConfig.getElement(config.getXmlDoc(), "database"), "control");
        return new DBConn(
                LoadConfig.getValue(control, "url"),
                LoadConfig.getValue(control, "username"),
                LoadConfig.getValue(control, "password")
        );
    }

    private static DBConn connectStagingDB(LoadConfig config) throws Exception {
        Element staging = LoadConfig.getChildElement(LoadConfig.getElement(config.getXmlDoc(), "database"), "staging");
        return new DBConn(
                LoadConfig.getValue(staging, "url"),
                LoadConfig.getValue(staging, "username"),
                LoadConfig.getValue(staging, "password")
        );
    }
}
