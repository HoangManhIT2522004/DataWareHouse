package scripts.extract_scripts;

import org.w3c.dom.Element;
import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ExtractToFile {

    private static DBConn controlDB;
    private static String currentExecutionId = null;

    /**
     * ============================================================
     * Step 1: Load cấu hình từ file XML
     * ============================================================
     */
    public static LoadConfig loadConfig(String path) {
        System.out.println("[Step 1] Loading configuration...");
        try {
            LoadConfig config = new LoadConfig(path);
            System.out.println("[Step 1] Config loaded from: " + path);
            return config;
        } catch (Exception e) {
            String subject = "ERROR: Load Configuration Failed";
            String body = "Không thể load file cấu hình từ đường dẫn: " + path +
                    "\n\nVui lòng kiểm tra:\n" +
                    "- File có tồn tại không?\n" +
                    "- Đường dẫn có chính xác không?\n" +
                    "- File có quyền đọc không?\n" +
                    "- Cấu trúc XML có hợp lệ không?";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 1] FAILED: Cannot load config file");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 2: Kết nối database
     * ============================================================
     */
    public static DBConn connectDB(LoadConfig config) {
        System.out.println("[Step 2] Connecting to database...");
        try {
            Element control = LoadConfig.getElement(config.getXmlDoc(), "control");
            String url = LoadConfig.getValue(control, "url");
            String username = LoadConfig.getValue(control, "username");
            String password = LoadConfig.getValue(control, "password");

            DBConn db = new DBConn(url, username, password);

            // Test connection
            db.executeQuery("SELECT 1", rs -> {
                if (rs.next()) {
                    System.out.println("[Step 2] Database connection successful");
                }
            });

            return db;

        } catch (Exception e) {
            String subject = "ERROR: Database Connection Failed";
            String body = "Không thể kết nối đến database.\n\n" +
                    "Vui lòng kiểm tra:\n" +
                    "- Database server có đang chạy không?\n" +
                    "- Thông tin kết nối (url, username, password) có chính xác không?\n" +
                    "- Network có kết nối được không?";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 2] FAILED: Cannot connect to database");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 3: Kiểm tra extract hôm nay
     * ============================================================
     */
    public static void checkTodayExtractSuccess() {
        try {
            System.out.println("[Step 3] Checking today's extract status...");
                String sql = "SELECT check_today_extract_success() AS success";
            final boolean[] alreadyExtracted = {false};

            controlDB.executeQuery(sql, rs -> {
                if (rs.next()) {
                    alreadyExtracted[0] = rs.getBoolean("success");
                }
            });

            if (alreadyExtracted[0]) {
                String subject = "Weather ETL - Extract Already Done Today";
                String body = "Hệ thống phát hiện hôm nay đã có tiến trình extract thành công.\n\n" +
                        "Thời gian: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                EmailSender.sendEmail(subject, body);
                System.out.println("[Step 3] Extract đã thực hiện hôm nay, dừng chương trình.");
                System.exit(0);
            } else {
                System.out.println("[Step 3] No extract success found today, proceeding...");
            }

        } catch (Exception e) {
            String subject = "ERROR: Check Today Extract Failed";
            String body = "Lỗi khi kiểm tra trạng thái extract hôm nay";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 3] FAILED: Error checking today's extract");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * ============================================================
     * Step 4: Chuẩn bị cho extract, Tạo/lấy config và tạo log mới
     * ============================================================
     */
    public static ExtractWeatherData.ExtractInfo prepareExtract(LoadConfig c) {
        try {
            System.out.println("[Step 4] Preparing extract...");

            Element config = LoadConfig.getElement(c.getXmlDoc(), "config");
            Element configSource = LoadConfig.getChildElement(config, "configSource");

            String baseName = LoadConfig.getValue(configSource, "config_name");
            String today = java.time.LocalDate.now().format(
                    java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")
            );
            String configName = baseName + "_" + today;
            String sourceType = LoadConfig.getValue(configSource, "source_type");
            String sourceUrl = LoadConfig.getValue(configSource, "source_url");
            String outputPath = LoadConfig.getValue(configSource, "output_path");
            boolean isActive = Boolean.parseBoolean(LoadConfig.getValue(configSource, "is_active"));

            System.out.println("[Step 4] Config info from XML:");
            System.out.println("  - Config Name   : " + configName);
            System.out.println("  - Source Type   : " + sourceType);
            System.out.println("  - Source URL    : " + sourceUrl);
            System.out.println("  - Output Path   : " + outputPath);
            System.out.println("  - Is Active     : " + isActive);

            // Bước 1: Lấy config
            String getConfigSql = String.format(
                    "SELECT * FROM get_or_create_config('%s', '%s', '%s', '%s', %b)",
                    configName, sourceType, sourceUrl, outputPath, isActive
            );

            final int[] configSrcId = {0};
            final String[] dbSourceUrl = {null};
            final String[] dbOutputPath = {null};

            controlDB.executeQuery(getConfigSql, rs -> {
                if (rs.next()) {
                    configSrcId[0] = rs.getInt("config_src_id");
                    dbSourceUrl[0] = rs.getString("source_url");
                    dbOutputPath[0] = rs.getString("output_path");
                }
            });

            if (configSrcId[0] == 0) {
                throw new Exception("Cannot get or create config");
            }

            System.out.println("[Step 4] Config from DB:");
            System.out.println("  - Config ID     : " + configSrcId[0]);
            System.out.println("  - Source URL    : " + dbSourceUrl[0]);
            System.out.println("  - Output Path   : " + dbOutputPath[0]);

            // Bước 2: Tạo log mới
            String createLogSql = String.format(
                    "SELECT create_new_log(%d) AS execution_id",
                    configSrcId[0]
            );

            final String[] executionId = {null};
            controlDB.executeQuery(createLogSql, rs -> {
                if (rs.next()) {
                    executionId[0] = rs.getString("execution_id");
                }
            });

            if (executionId[0] == null) {
                throw new Exception("Cannot create log");
            }

            // Lưu execution_id để dùng trong finally
            currentExecutionId = executionId[0];

            System.out.println("[Step 4] Created execution: " + executionId[0]);

            return new ExtractWeatherData.ExtractInfo(
                    executionId[0],
                    dbSourceUrl[0],
                    dbOutputPath[0]
            );

        } catch (Exception e) {
            EmailSender.sendError("ERROR: Prepare Extract Failed", e.getMessage(), e);
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Update log status về SUCCESS
     * ============================================================
     */
    public static void updateLogSuccess(String executionId, int recordsCount) {
        try {
            System.out.println("[Step 6] Updating log status to SUCCESS...");

            // Sử dụng CALL thay vì SELECT cho procedure
            String sql = String.format(
                    "CALL update_log_status('%s', 'success'::src_status, %d, NULL)",
                    executionId, recordsCount
            );

            // Nếu CALL không work, thử dùng DO block
            String alternativeSql = String.format(
                    "DO $ BEGIN PERFORM update_log_status('%s', 'success'::src_status, %d, NULL); END $",
                    executionId, recordsCount
            );

            try {
                controlDB.executeUpdate(sql);
            } catch (Exception e1) {
                // Fallback: Dùng UPDATE trực tiếp
                String directUpdate = String.format(
                        "UPDATE log_src SET status='success'::src_status, end_time=NOW(), " +
                                "records_extracted=%d, error_message=NULL WHERE execution_id='%s'",
                        recordsCount, executionId
                );
                controlDB.executeUpdate(directUpdate);
            }

            System.out.println("[Step 6] Log status updated to SUCCESS");
        } catch (Exception e) {
            System.err.println("[Step 6] FAILED: Cannot update log status");
            e.printStackTrace();
        }
    }

    /**
     * ============================================================
     * Update log status về FAILED
     * ============================================================
     */
    public static void updateLogFailed(String executionId, String errorMessage) {
        try {
            System.out.println("[Cleanup] Updating log status to FAILED...");

            // Escape single quotes
            String escapedError = errorMessage != null ? errorMessage.replace("'", "''") : "";

            // Dùng UPDATE trực tiếp để tránh vấn đề function signature
            String sql = String.format(
                    "UPDATE log_src SET status='failed'::src_status, end_time=NOW(), " +
                            "records_extracted=0, error_message='%s' WHERE execution_id='%s'",
                    escapedError, executionId
            );

            controlDB.executeUpdate(sql);
            System.out.println("[Cleanup] Log status updated to FAILED");
        } catch (Exception e) {
            System.err.println("[Cleanup] Cannot update log status to FAILED: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("Weather ETL - Extract Process Started");
        System.out.println("========================================\n");

        ExtractWeatherData.ExtractInfo extractInfo = null;

        try {
            // Step 1: Load config chung (database connection)
            LoadConfig config = loadConfig("D:/DataWareHouse/src/main/java/config/config.xml");

            // Step 2: Connect DB
            controlDB = connectDB(config);

            // Step 3: Check extract hôm nay
            checkTodayExtractSuccess();

            // Step 4: Load extract config riêng (locations, api, config source)
            LoadConfig extractConfig = loadConfig("D:/DataWareHouse/src/main/java/scripts/extract_scripts/extract_config.xml");

            // Step 5: Lấy/Tạo config và tạo log
            extractInfo = prepareExtract(extractConfig);

            // Step 6: Extract weather data
            int recordsExtracted = ExtractWeatherData.extractWeatherToFile(
                    extractConfig,  // ← Dùng extractConfig (có locations)
                    extractInfo.getExecutionId(),
                    extractInfo.getSourceUrl(),
                    extractInfo.getOutputPath()
            );

            // Step 7: Update log status = SUCCESS
            updateLogSuccess(extractInfo.getExecutionId(), recordsExtracted);

            // Step 8: Gửi email thành công
            String subject = "✓ Weather ETL - Extract Completed Successfully";
            String body = String.format(
                    "=== EXTRACT SUCCESS ===\n\n" +
                            "Execution ID: %s\n" +
                            "Records Extracted: %d\n" +
                            "Output File: %s\n" +
                            "Completion Time: %s\n\n" +
                            "Status: SUCCESS",
                    extractInfo.getExecutionId(),
                    recordsExtracted,
                    extractInfo.getOutputPath(),
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            );
            EmailSender.sendEmail(subject, body);

            System.out.println("\n========================================");
            System.out.println("Weather ETL - Extract Process COMPLETED");
            System.out.println("========================================");
            System.exit(0);

        } catch (Exception e) {
            // Update log status = FAILED
            if (currentExecutionId != null) {
                updateLogFailed(currentExecutionId, e.getMessage());
            }

            // Gửi email lỗi
            String subject = "✗ ERROR: Weather ETL - Extract Failed";
            String body = String.format(
                    "=== EXTRACT FAILED ===\n\n" +
                            "Execution ID: %s\n" +
                            "Error Time: %s\n" +
                            "Error Message: %s\n\n" +
                            "Stack Trace:\n%s\n\n" +
                            "Status: FAILED",
                    currentExecutionId != null ? currentExecutionId : "N/A",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                    e.getMessage(),
                    getStackTraceString(e)
            );
            EmailSender.sendError(subject, body, e);

            System.err.println("\n========================================");
            System.err.println("Weather ETL - Extract Process FAILED");
            System.err.println("========================================");
            e.printStackTrace();
            System.exit(1);

        }
    }

    /**
     * Helper method để lấy stack trace dạng String
     */
    private static String getStackTraceString(Exception e) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append("  at ").append(element.toString()).append("\n");
            if (sb.length() > 1000) { // Giới hạn độ dài
                sb.append("  ...");
                break;
            }
        }
        return sb.toString();
    }
}