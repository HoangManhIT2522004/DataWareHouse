package scripts.load_scripts;

import org.w3c.dom.Element;
import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LoadToDataWarehouse {

    private static DBConn controlDB;
    private static DBConn warehouseDB;
    private static String currentExecutionId;
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter idFormat = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MINUTES = 15;

    public static void main(String[] args) {
        int attempt = 1;

        while (attempt <= MAX_RETRIES) {
            System.out.println("\n=== LẦN CHẠY THỨ " + attempt + " / " + MAX_RETRIES + " ===");
            System.out.println("Thời gian bắt đầu: " + LocalDateTime.now().format(dtf));

            controlDB = null;
            warehouseDB = null;
            currentExecutionId = null;

            try {
                LoadConfig config = new LoadConfig("D:/DataWareHouse/src/main/java/config/config.xml");

                controlDB   = connectControl(config);
                warehouseDB = connectWarehouse(config);

                currentExecutionId = createNewLoadLog();
                System.out.println("→ Execution ID: " + currentExecutionId + "\n");

                int loc  = call("load_dim_location");
                int cond = call("load_dim_weather_condition");
                int fact = call("load_fact_weather_daily");
                int aqi  = call("load_fact_air_quality_daily");

                int total = loc + cond + fact + aqi;

                updateLogSuccess(total);
                sendSuccessEmail(loc, cond, fact, aqi, total, attempt);

                System.out.println("\nLOAD HOÀN TẤT THÀNH CÔNG! (lần thứ " + attempt + ")");
                System.out.println("Tổng cộng: " + String.format("%,d", total) + " bản ghi đã được load vào Warehouse");
                System.exit(0);

            } catch (Exception e) {
                System.err.println("LẦN " + attempt + " THẤT BẠI: " + e.getMessage());
                e.printStackTrace();

                if (currentExecutionId != null) {
                    try { updateLogFailed("Lần " + attempt + ": " + e.toString()); } catch (Exception ignored) {}
                }
                sendErrorEmail(e, attempt);

                if (attempt < MAX_RETRIES) {
                    System.out.println("Đang chờ " + RETRY_DELAY_MINUTES + " phút để thử lại lần " + (attempt + 1) + "...");
                    try {
                        Thread.sleep(RETRY_DELAY_MINUTES * 60 * 1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        System.out.println("Bị gián đoạn khi chờ retry!");
                        System.exit(1);
                    }
                } else {
                    System.out.println("\nĐÃ THỬ " + MAX_RETRIES + " LẦN NHƯNG VẪN LỖI → DỪNG HOÀN TOÀN!");
                    System.exit(1);
                }
            }

            attempt++;
        }
    }

    // ================== CÁC HÀM HỖ TRỢ (GIỮ NGUYÊN) ==================

    private static DBConn connectControl(LoadConfig c) throws Exception {
        Element e = LoadConfig.getElement(c.getXmlDoc(), "control");
        return connect(e, "Control DB");
    }

    private static DBConn connectWarehouse(LoadConfig c) throws Exception {
        Element e = LoadConfig.getElement(c.getXmlDoc(), "datawarehouse");
        return connect(e, "DataWarehouse DB");
    }

    private static DBConn connect(Element e, String name) throws Exception {
        String url  = LoadConfig.getValue(e, "url");
        String user = LoadConfig.getValue(e, "username");
        String pass = LoadConfig.getValue(e, "password");
        DBConn db = new DBConn(url, user, pass);
        db.executeQuery("SELECT 1", rs -> {});
        System.out.println("Kết nối thành công: " + name);
        return db;
    }

    private static String createNewLoadLog() throws Exception {
        String baseId = "LOAD_" + LocalDateTime.now().format(idFormat);
        String newId = baseId;
        int counter = 1;

        while (true) {
            try {
                controlDB.executeUpdate(
                        "INSERT INTO log_process (execution_id, start_time, status) VALUES (?, NOW(), 'running'::process_status)",
                        newId
                );
                break;
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("duplicate key")) {
                    newId = baseId + "_" + counter++;
                } else {
                    throw e;
                }
            }
        }
        return newId;
    }

    private static int call(String functionName) throws Exception {
        System.out.println("Đang gọi function: " + functionName + " ...");
        final int[] count = {0};
        warehouseDB.executeQuery("SELECT " + functionName + "() AS rows", rs -> {
            if (rs.next()) count[0] = rs.getInt("rows");
        });
        System.out.println("   Hoàn thành: " + String.format("%,d", count[0]) + " bản ghi");
        return count[0];
    }

    private static void updateLogSuccess(int total) throws Exception {
        controlDB.executeUpdate(
                "UPDATE log_process SET status = 'success'::process_status, records_inserted = ?, end_time = NOW() WHERE execution_id = ?",
                total, currentExecutionId
        );
    }

    private static void updateLogFailed(String msg) {
        try {
            controlDB.executeUpdate(
                    "UPDATE log_process SET status = 'failed'::process_status, error_message = ?, end_time = NOW() WHERE execution_id = ?",
                    msg.length() > 2000 ? msg.substring(0, 2000) : msg, currentExecutionId
            );
        } catch (Exception ignored) {}
    }

    // ================== EMAIL BÁO CÁO MỚI - ĐẸP NHƯ EXTRACT ==================

    private static void sendSuccessEmail(int loc, int cond, int fact, int aqi, int total, int attempt) {
        String subject = "Weather ETL - LOAD TO WAREHOUSE THÀNH CÔNG " +
                (attempt > 1 ? "(Sau " + (attempt - 1) + " lần retry)" : "");

        StringBuilder body = new StringBuilder();
        body.append("======================================\n");
        body.append("   WEATHER ETL - LOAD TO DATA WAREHOUSE   \n");
        body.append("          HOÀN TẤT THÀNH CÔNG           \n");
        body.append("======================================\n\n");

        body.append("Execution ID       : ").append(currentExecutionId).append("\n");
        body.append("Thời gian kết thúc : ").append(LocalDateTime.now().format(dtf)).append("\n");
        body.append("Số lần thử         : ").append(attempt)
                .append(attempt > 1 ? " (tự động retry thành công)" : " (thành công ngay lần đầu)").append("\n");
        body.append("Tổng bản ghi       : ").append(String.format("%,d", total)).append(" records\n\n");

        body.append("--- CHI TIẾT LOAD ---\n");
        body.append(String.format("✓ load_dim_location          : %,d bản ghi\n", loc));
        body.append(String.format("✓ load_dim_weather_condition : %,d bản ghi\n", cond));
        body.append(String.format("✓ load_fact_weather_daily    : %,d bản ghi\n", fact));
        body.append(String.format("✓ load_fact_air_quality_daily: %,d bản ghi\n", aqi));
        body.append("\n");

        body.append("Tất cả dữ liệu đã được load thành công vào Data Warehouse.\n");
        body.append("Hệ thống đang hoạt động ổn định.\n\n");

        body.append("Thời gian báo cáo: ")
                .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")));

        EmailSender.sendEmail(subject, body.toString());
        System.out.println("Đã gửi email báo thành công!");
    }

    private static void sendErrorEmail(Exception e, int attempt) {
        boolean isFinalFailure = attempt >= MAX_RETRIES;

        String subject = isFinalFailure
                ? "Weather ETL - LOAD TO WAREHOUSE THẤT BẠI HOÀN TOÀN (sau " + MAX_RETRIES + " lần)"
                : "Weather ETL - LOAD TO WAREHOUSE THẤT BẠI (lần " + attempt + "/" + MAX_RETRIES + ")";

        StringBuilder body = new StringBuilder();
        body.append("======================================\n");
        body.append("   WEATHER ETL - LOAD TO DATA WAREHOUSE   \n");
        body.append(isFinalFailure ? "        THẤT BẠI HOÀN TOÀN        \n" : "          ĐANG CÓ LỖI (retry)         \n");
        body.append("======================================\n\n");

        body.append("Execution ID : ").append(currentExecutionId != null ? currentExecutionId : "N/A").append("\n");
        body.append("Lần thử      : ").append(attempt).append("/").append(MAX_RETRIES).append("\n");
        body.append("Thời gian    : ").append(LocalDateTime.now().format(dtf)).append("\n\n");

        body.append("--- THÔNG TIN LỖI ---\n");
        body.append("Message : ").append(e.getMessage() != null ? e.getMessage() : "null").append("\n");
        body.append("Class   : ").append(e.getClass().getName()).append("\n\n");

        body.append("--- STACK TRACE (5 dòng đầu) ---\n");
        StackTraceElement[] stack = e.getStackTrace();
        for (int i = 0; i < Math.min(5, stack.length); i++) {
            body.append("   at ").append(stack[i].toString()).append("\n");
        }
        if (stack.length > 5) body.append("   ... (và ").append(stack.length - 5).append(" dòng nữa)\n");

        body.append("\n");

        if (!isFinalFailure) {
            body.append("Hệ thống sẽ tự động thử lại lần ").append(attempt + 1)
                    .append(" sau ").append(RETRY_DELAY_MINUTES).append(" phút.\n");
            body.append("Không cần can thiệp ngay lúc này.\n");
        } else {
            body.append("ĐÃ THỬ HẾT ").append(MAX_RETRIES).append(" LẦN → DỪNG HOÀN TOÀN!\n");
            body.append("Cần kiểm tra khẩn cấp:\n");
            body.append("- Kết nối Control DB / Data Warehouse?\n");
            body.append("- Các function load_* có bị lỗi logic?\n");
            body.append("- Quyền truy cập database?\n");
            body.append("- Transaction log full / disk full?\n");
            body.append("- Network timeout?\n");
        }

        body.append("\nThời gian báo cáo: ")
                .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")));

        EmailSender.sendError(subject, body.toString(), e);
        System.out.println("Đã gửi email báo lỗi!");
    }
}