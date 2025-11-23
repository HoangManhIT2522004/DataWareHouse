package scripts.load_scripts;

import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;
import org.w3c.dom.Element;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LoadToDataWarehouse {

    private static DBConn controlDB;
    private static DBConn warehouseDB;
    private static String loadExecutionId;

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MINUTES = 15;

    public static void main(String[] args) {
        int attempt = 1;

        while (attempt <= MAX_RETRIES) {
            System.out.println("\n========================================");
            System.out.println("   WEATHER ETL - STEP 7: LOAD TO WAREHOUSE");
            System.out.println("   Lần chạy thứ: " + attempt + "/" + MAX_RETRIES);
            System.out.println("   Thời gian: " + LocalDateTime.now().format(dtf));
            System.out.println("========================================");

            controlDB = null;
            warehouseDB = null;
            loadExecutionId = null;

            try {
                // 1. Load config (giống hệt load_to_staging)
                LoadConfig config = new LoadConfig("config/config.xml");

                // 2. Kết nối DB
                controlDB = connectControlDB(config);
                warehouseDB = connectWarehouseDB(config);

                // 3. KIỂM TRA HÔM NAY ĐÃ CHẠY LOAD WAREHOUSE CHƯA (idempotent)
                checkTodayWarehouseLoadSuccess();

                // 4. Tạo log execution chính thức (EXEC-20251122-001)
                loadExecutionId = prepareWarehouseLoadProcess();

                // 5. Thực hiện Load vào Warehouse
                int loc  = callLoadFunction("load_dim_location");
                int cond = callLoadFunction("load_dim_weather_condition");
                int fact = callLoadFunction("load_fact_weather_daily");
                int aqi  = callLoadFunction("load_fact_air_quality_daily");

                int total = loc + cond + fact + aqi;

                // 6. Cập nhật log thành công
                updateProcessLogStatus(loadExecutionId, "success", total, 0, "Loaded to Warehouse successfully");

                // 7. Gửi email báo thành công
                sendSuccessEmail(loc, cond, fact, aqi, total, attempt);

                System.out.println("\nLOAD TO DATA WAREHOUSE HOÀN TẤT THÀNH CÔNG!");
                System.out.println("Tổng cộng: " + String.format("%,d", total) + " bản ghi");
                System.out.println("Execution ID: " + loadExecutionId);
                System.out.println("========================================");
                System.exit(0);

            } catch (Exception e) {
                System.err.println("LẦN " + attempt + " THẤT BẠI: " + e.getMessage());
                e.printStackTrace();

                if (loadExecutionId != null) {
                    try {
                        updateProcessLogStatus(loadExecutionId, "failed", 0, 0,
                                "Attempt " + attempt + " failed: " + e.getMessage());
                    } catch (Exception ignored) {}
                }

                sendErrorEmail(e, attempt);

                if (attempt == MAX_RETRIES) {
                    System.err.println("ĐÃ THỬ " + MAX_RETRIES + " LẦN - DỪNG HOÀN TOÀN!");
                    System.exit(1);
                }

                System.out.println("Chờ " + RETRY_DELAY_MINUTES + " phút để thử lại lần " + (attempt + 1) + "...");
                try {
                    Thread.sleep(RETRY_DELAY_MINUTES * 60 * 1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.exit(1);
                }
            }
            attempt++;
        }
    }

    // ============================================================
    // CONNECTION & CONFIG (GIỐNG HỆT load_to_staging)
    // ============================================================

    private static DBConn connectControlDB(LoadConfig config) throws Exception {
        Element control = LoadConfig.getElement(config.getXmlDoc(), "control");
        return new DBConn(
                LoadConfig.getValue(control, "url"),
                LoadConfig.getValue(control, "username"),
                LoadConfig.getValue(control, "password")
        );
    }

    private static DBConn connectWarehouseDB(LoadConfig config) throws Exception {
        Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
        Element warehouse = LoadConfig.getChildElement(database, "datawarehouse");
        return new DBConn(
                LoadConfig.getValue(warehouse, "url"),
                LoadConfig.getValue(warehouse, "username"),
                LoadConfig.getValue(warehouse, "password")
        );
    }

    // ============================================================
    // IDEMPOTENT CHECK - Giống hệt load_to_staging
    // ============================================================

    private static void checkTodayWarehouseLoadSuccess() throws Exception {
        System.out.println("[Check] Kiểm tra xem hôm nay đã Load Warehouse chưa...");
        String sql = "SELECT check_today_process_success('WA_Load_WH') AS success";
        final boolean[] alreadyDone = {false};

        controlDB.executeQuery(sql, rs -> {
            if (rs.next()) alreadyDone[0] = rs.getBoolean("success");
        });

        if (alreadyDone[0]) {
            String msg = "Hôm nay đã chạy Load to Data Warehouse thành công rồi. Dừng tiến trình để tránh duplicate.";
            System.out.println(msg);
            EmailSender.sendEmail("ETL Notification: Load Warehouse Already Done Today", msg);
            System.exit(0);
        } else {
            System.out.println("Chưa chạy Load Warehouse hôm nay. Tiếp tục...");
        }
    }

    // ============================================================
    // LOGGING - DÙNG CHUẨN controlDB (giống load_to_staging)
    // ============================================================

    private static String prepareWarehouseLoadProcess() throws Exception {
        System.out.println("[Log] Tạo log process cho Load Warehouse...");
        String processName = "WA_Load_WH_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        // Tạo hoặc lấy config_process
        String sqlConfig = String.format(
                "SELECT get_or_create_config_process('%s', 'load_warehouse', 'staging_to_warehouse', 'datawarehouse', 'dim_and_fact_tables')",
                processName
        );

        final int[] configId = {0};
        controlDB.executeQuery(sqlConfig, rs -> {
            if (rs.next()) configId[0] = rs.getInt(1);
        });
        if (configId[0] == 0) throw new Exception("Không tạo được config_process");

        // Tạo execution log
        String sqlLog = "SELECT create_new_process_log(" + configId[0] + ")";
        final String[] execId = {null};
        controlDB.executeQuery(sqlLog, rs -> {
            if (rs.next()) execId[0] = rs.getString(1);
        });

        if (execId[0] == null) throw new Exception("Không tạo được execution log");

        System.out.println("Execution ID: " + execId[0]);
        return execId[0];
    }

    private static void updateProcessLogStatus(String execId, String status, int inserted, int failed, String message) {
        try {
            String sql = "SELECT update_process_log_status(?, ?::process_status, ?, ?, ?)";
            controlDB.executeQuery(sql, rs -> {}, execId, status, inserted, failed, message);
            System.out.println("[Log] Đã cập nhật trạng thái: " + status);
        } catch (Exception e) {
            System.err.println("Không cập nhật được log: " + e.getMessage());
        }
    }

    // ============================================================
    // GỌI CÁC FUNCTION TRONG WAREHOUSE
    // ============================================================

    private static int callLoadFunction(String functionName) throws Exception {
        System.out.println("Đang thực thi: " + functionName + "() ...");
        final int[] count = {0};
        warehouseDB.executeQuery("SELECT " + functionName + "() AS rows", rs -> {
            if (rs.next()) count[0] = rs.getInt("rows");
        });
        System.out.println("   Hoàn thành: " + String.format("%,d", count[0]) + " bản ghi");
        return count[0];
    }

    // ============================================================
    // EMAIL BÁO CÁO (GIỮ NGUYÊN ĐẸP NHƯ CŨ)
    // ============================================================

    private static void sendSuccessEmail(int loc, int cond, int fact, int aqi, int total, int attempt) {
        String subject = "Weather ETL - LOAD TO WAREHOUSE THÀNH CÔNG " +
                (attempt > 1 ? "(sau " + (attempt-1) + " lần retry)" : "");

        String body = """
            ======================================
               WEATHER ETL - LOAD TO DATA WAREHOUSE   
                      HOÀN TẤT THÀNH CÔNG           
            ======================================
            
            Execution ID       : %s
            Thời gian          : %s
            Số lần thử         : %d %s
            Tổng bản ghi       : %,d records
            
            --- CHI TIẾT ---
            ✓ dim_location           : %,d
            ✓ dim_weather_condition  : %,d
            ✓ fact_weather_daily     : %,d
            ✓ fact_air_quality_daily : %,d
            
            Dữ liệu đã được load đầy đủ vào Data Warehouse.
            Hệ thống hoạt động ổn định!
            
            Báo cáo tự động - %s
            """.formatted(
                loadExecutionId,
                LocalDateTime.now().format(dtf),
                attempt,
                attempt > 1 ? "(retry thành công)" : "(lần đầu tiên)",
                total,
                loc, cond, fact, aqi,
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"))
        );

        EmailSender.sendEmail(subject, body);
        System.out.println("Đã gửi email báo thành công!");
    }

    private static void sendErrorEmail(Exception e, int attempt) {
        String subject = attempt >= MAX_RETRIES
                ? "Weather ETL - LOAD TO WAREHOUSE THẤT BẠI HOÀN TOÀN"
                : "Weather ETL - LOAD TO WAREHOUSE THẤT BẠI (lần " + attempt + "/" + MAX_RETRIES + ")";

        String body = """
            ======================================
               WEATHER ETL - LOAD TO DATA WAREHOUSE   
                      %s                     
            ======================================
            
            Execution ID : %s
            Lần thử      : %d/%d
            Thời gian    : %s
            
            Lỗi: %s
            
            %s
            """.formatted(
                attempt >= MAX_RETRIES ? "THẤT BẠI HOÀN TOÀN" : "ĐANG LỖI - SẼ RETRY",
                loadExecutionId != null ? loadExecutionId : "N/A",
                attempt, MAX_RETRIES,
                LocalDateTime.now().format(dtf),
                e.getMessage(),
                attempt < MAX_RETRIES
                        ? "Sẽ tự động thử lại sau " + RETRY_DELAY_MINUTES + " phút..."
                        : "ĐÃ HẾT LẦN THỬ - CẦN CAN THIỆP KHẨN!"
        );

        EmailSender.sendError(subject, body, e);
        System.out.println("Đã gửi email báo lỗi!");
    }
}