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
                sendSuccessEmail(total, attempt);

                System.out.println("\nLOAD HOÀN TẤT THÀNH CÔNG! (lần thứ " + attempt + ")");
                System.out.println("Tổng cộng: " + total + " bản ghi đã được load vào Warehouse");
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
            // KHÔNG CẦN finally ĐÓNG GÌ CẢ – try-with-resources trong DBConn đã lo hết rồi!

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
        System.out.println("   Hoàn thành: " + count[0] + " bản ghi");
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

    private static void sendSuccessEmail(int total, int attempt) {
        String body = """
            WEATHER ETL - LOAD TO WAREHOUSE THÀNH CÔNG
            
            Execution ID   : %s
            Thời gian      : %s
            Số lần thử     : %d
            Tổng bản ghi   : %,d
            
            Trạng thái: SUCCESS %s
            """.formatted(currentExecutionId, LocalDateTime.now().format(dtf), attempt, total,
                (attempt > 1 ? "(sau khi tự động retry)" : ""));
        EmailSender.sendEmail("Weather ETL - Load Warehouse SUCCESS", body);
    }

    private static void sendErrorEmail(Exception e, int attempt) {
        String status = attempt < MAX_RETRIES
                ? "Hệ thống sẽ tự động thử lại sau 15 phút..."
                : "ĐÃ THỬ HẾT " + MAX_RETRIES + " LẦN → DỪNG HOÀN TOÀN!";

        String body = """
            WEATHER ETL - LOAD TO WAREHOUSE THẤT BẠI (lần %d/%d)
            
            Execution ID : %s
            Thời gian    : %s
            Lỗi          : %s
            
            %s
            """.formatted(attempt, MAX_RETRIES,
                currentExecutionId != null ? currentExecutionId : "N/A",
                LocalDateTime.now().format(dtf), e.getMessage(), status);

        EmailSender.sendError("Weather ETL - Load Warehouse FAILED (lần " + attempt + ")", body, e);
    }
}