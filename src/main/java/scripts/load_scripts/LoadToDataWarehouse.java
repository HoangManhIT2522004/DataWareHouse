package scripts.load_scripts;

import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;
import org.w3c.dom.Element;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LoadToDataWarehouse {

    private static DBConn controlDB;
    private static DBConn stagingDB;
    private static DBConn warehouseDB;
    private static String executionId;

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter dtf_date = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("   WEATHER ETL - STEP 4: LOAD TO WAREHOUSE");
        System.out.println("========================================\n");

        try {
            // 1. Load config
            LoadConfig config = new LoadConfig("config/config.xml");
            controlDB   = connectControlDB(config);
            stagingDB   = connectStagingDB(config);
            warehouseDB = connectWarehouseDB(config);

            // 2. Idempotent check
            checkTodayWarehouseLoadSuccess();

            // 3. Tạo execution log
            executionId = prepareWarehouseLoadProcess();

            // 4. Load dữ liệu (Đã sửa thành Incremental Load)
            int total = loadToWarehouse(executionId);

            // 5. Cập nhật log
            updateProcessLogStatus(executionId, "success", total, 0,
                    "Load to Data Warehouse successfully");

            System.out.println("\nTổng bản ghi đã load: " + total);
            // 6. Gửi email báo thành công
            sendSuccessEmail(total);

            System.out.println("\nLOAD TO DATA WAREHOUSE COMPLETED SUCCESSFULLY");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("\nLOAD TO DATA WAREHOUSE FAILED");
            e.printStackTrace();

            if (executionId != null)
                updateProcessLogStatus(executionId, "failed", 0, 0, e.getMessage());

            sendErrorEmail(e);
            System.exit(1);
        }
    }

    // ========================================================================
    // (1) CHECK TODAY & (2) LOG PROCESS - ĐÃ SỬA
    // ========================================================================
    private static void checkTodayWarehouseLoadSuccess() throws Exception {
        System.out.println("[Check] Kiểm tra xem hôm nay đã Load Warehouse chưa...");
        String sql = "SELECT check_today_success_loadwarehouse(?) AS success";
        final boolean[] alreadyDone = {false};

        // SỬA ĐỔI: Sử dụng "LOD_DW" làm tiền tố.
        // Hàm PL/pgSQL sử dụng LIKE 'LOD_DW' || '%' để khớp với 'LOD_DW_YYYYMMDD'
        controlDB.executeQuery(sql, rs -> {
            if (rs.next()) alreadyDone[0] = rs.getBoolean("success");
        }, "LOD_DW"); // <<< Đã sửa thành "LOD_DW" (Bỏ dấu gạch dưới cuối)

        if (alreadyDone[0]) {
            String msg = "Hôm nay đã chạy Load to Data Warehouse thành công rồi. Dừng tiến trình để tránh duplicate.";
            System.out.println(msg);
            EmailSender.sendEmail("ETL Notification: Load Warehouse Already Done Today", msg);
            System.exit(0);
        } else {
            System.out.println("Chưa chạy Load Warehouse hôm nay. Tiếp tục...");
        }
    }

    private static String prepareWarehouseLoadProcess() throws Exception {
        System.out.println("[Log] Tạo log process cho Load Warehouse...");

        // Tiền tố LOD_DW đã được sửa ở bước trước
        String processName = "LOD_DW_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        // Tạo hoặc lấy config_process
        // SỬA ĐỔI: Thay 'datawarehouse' bằng 'warehouse' trong tham số thứ 4
        String sqlConfig = String.format(
                "SELECT get_or_create_process_loadwarehouse('%s', 'load_warehouse', 'staging_to_warehouse', 'warehouse', 'dim_and_fact_tables')",
                processName
        );

        final int[] configId = {0};
        controlDB.executeQuery(sqlConfig, rs -> {
            if (rs.next()) configId[0] = rs.getInt(1);
        });
        if (configId[0] == 0) throw new Exception("Không tạo được config_process");

        // Tạo execution log
        String sqlLog = "SELECT create_new_log_loadwarehouse(" + configId[0] + ")";
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
            // SỬA: Thay thế 'update_process_log_status' bằng 'update_log_status_loadwarehouse'
            String sql = "SELECT update_log_status_loadwarehouse(?, ?::process_status, ?, ?, ?)";
            // Lưu ý: Hàm update_log_status_loadwarehouse trả về VOID, nhưng ta vẫn dùng executeQuery
            // với ResultSetHandler rỗng.
            controlDB.executeQuery(sql, rs -> {}, execId, status, inserted, failed, message);
            System.out.println("[Log] Đã cập nhật trạng thái: " + status);
        } catch (Exception e) {
            System.err.println("Không cập nhật được log: " + e.getMessage());
        }
    }

    // ========================================================================
    // CÁC HÀM KHÁC - GIỮ NGUYÊN
    // ========================================================================
    private static void sendSuccessEmail(int total) {
        String subject = "Weather ETL - LOAD TO DATA WAREHOUSE THÀNH CÔNG";

        String body = """
                ======================================
                   WEATHER ETL - LOAD TO WAREHOUSE
                          THÀNH CÔNG
                ======================================

                Execution ID : %s
                Thời gian    : %s
                Tổng bản ghi : %,d records

                Hệ thống hoạt động ổn định.
                """.formatted(
                executionId,
                LocalDateTime.now().format(dtf),
                total
        );

        EmailSender.sendEmail(subject, body);
        System.out.println("[Email] Đã gửi email thành công!");
    }

    private static void sendErrorEmail(Exception e) {
        String subject = "Weather ETL - LOAD TO WAREHOUSE THẤT BẠI";

        String body = """
                ======================================
                   WEATHER ETL - LOAD TO WAREHOUSE
                              THẤT BẠI
                ======================================

                Execution ID : %s
                Thời gian    : %s

                Lỗi:
                %s

                Cần kiểm tra ngay.
                """
                .formatted(
                        executionId != null ? executionId : "N/A",
                        LocalDateTime.now().format(dtf),
                        e.getMessage()
                );

        EmailSender.sendError(subject, body, e);
        System.out.println("[Email] Đã gửi email báo lỗi!");
    }


    private static int loadToWarehouse(String execId) throws Exception {
        System.out.println("[Process] Starting Load to Warehouse ...");
        int total = 0;

        try (Connection connStaging = stagingDB.getConnection();
             Connection connWarehouse = warehouseDB.getConnection()) {

            connWarehouse.setAutoCommit(false);

            // 1. DIMENSION TABLES: Load/Merge Incremental
            total += loadDimensionIncremental(connWarehouse, connStaging, "dim_location", "location_id");
            total += loadDimensionIncremental(connWarehouse, connStaging, "dim_weather_condition", "condition_id");

            // 2. FACT TABLES: Load chỉ dữ liệu hôm nay (Incremental/Daily Refresh)
            total += loadFactDailyIncremental(connWarehouse, connStaging, "fact_weather_daily", "observation_date");
            total += loadFactDailyIncremental(connWarehouse, connStaging, "fact_air_quality_daily", "observation_time");

            connWarehouse.commit();
        }
        return total;
    }

    private static int loadFactDailyIncremental(Connection warehouse, Connection staging,
                                                String tableName, String dateColumnName) throws SQLException {

        String todayDate = LocalDateTime.now().format(dtf_date);
        int totalRows = 0;

        System.out.print(" Loading  " + tableName + " : ");

        String whereClause;
        if (dateColumnName.equals("observation_time")) {
            whereClause = String.format("DATE(%s) = CAST(? AS DATE)", dateColumnName);
        } else {
            whereClause = String.format("%s = CAST(? AS DATE)", dateColumnName);
        }

        // B1. XÓA dữ liệu ngày hôm nay trong Warehouse (để xử lý re-run)
        try (Statement delStmt = warehouse.createStatement()) {
            String castedDate = "'" + todayDate + "'::DATE";
            String deleteSql;
            if (dateColumnName.equals("observation_time")) {
                deleteSql = String.format("DELETE FROM %s WHERE DATE(%s) = %s", tableName, dateColumnName, castedDate);
            } else {
                deleteSql = String.format("DELETE FROM %s WHERE %s = %s", tableName, dateColumnName, castedDate);
            }

            int deletedRows = delStmt.executeUpdate(deleteSql);

        }

        String columns = getColumnList(staging, tableName);
        String placeholders = columns.replaceAll("[^,]+", "?");
        String insertSql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
        String selectSql = "SELECT " + columns + " FROM " + tableName + " WHERE " + whereClause;

        try (PreparedStatement ps = warehouse.prepareStatement(insertSql);
             PreparedStatement sel = staging.prepareStatement(selectSql)) {

            sel.setString(1, todayDate);

            int batch = 0;
            try (ResultSet rs = sel.executeQuery()) {
                while (rs.next()) {
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        ps.setObject(i, rs.getObject(i));
                    }
                    ps.addBatch();
                    batch++;

                    if (batch >= 5000) {
                        totalRows += ps.executeBatch().length;
                        batch = 0;
                    }
                }
                if (batch > 0) totalRows += ps.executeBatch().length;
            }
        }

        // Kết thúc dòng in bằng tổng số bản ghi
        System.out.println(totalRows + " rows.");
        return totalRows;
    }

    private static int loadDimensionIncremental(Connection warehouse, Connection staging,
                                                String tableName, String pkColumnName) throws SQLException {

        System.out.print(" Loading  " + tableName + " : ");
        int totalRows = 0;

        String columns = getColumnList(staging, tableName);
        String placeholders = columns.replaceAll("[^,]+", "?");

        String insertSql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ") ON CONFLICT (" + pkColumnName + ") DO NOTHING";

        try (PreparedStatement ps = warehouse.prepareStatement(insertSql);
             Statement sel = staging.createStatement()) {

            try (ResultSet rs = sel.executeQuery("SELECT " + columns + " FROM " + tableName)) {
                int batch = 0;
                while (rs.next()) {
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        ps.setObject(i, rs.getObject(i));
                    }
                    ps.addBatch();
                    batch++;

                    if (batch >= 5000) {
                        totalRows += ps.executeBatch().length;
                        batch = 0;
                    }
                }
                if (batch > 0) totalRows += ps.executeBatch().length;
            }
        }

        System.out.println(totalRows + " rows.");
        return totalRows;
    }

    // ========================================================================
    // HÀM PHỤ TRỢ: Get Column List - GIỮ NGUYÊN
    // ========================================================================
    private static String getColumnList(Connection conn, String tableName) throws SQLException {
        String sql =
                "SELECT string_agg('\"' || column_name || '\"', ',') " +
                        "FROM information_schema.columns " +
                        "WHERE table_name = ? AND table_schema = current_schema()";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getString(1) != null)
                    return rs.getString(1);
            }
        }

        throw new SQLException("Cannot get columns for table: " + tableName);
    }

    // ========================================================================
    // (5) CONNECT DB - GIỮ NGUYÊN
    // ========================================================================
    private static DBConn connectWarehouseDB(LoadConfig config) throws Exception {
        Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
        Element warehouse = LoadConfig.getChildElement(database, "warehouse");
        return new DBConn(
                LoadConfig.getValue(warehouse, "url"),
                LoadConfig.getValue(warehouse, "username"),
                LoadConfig.getValue(warehouse, "password")
        );
    }

    private static DBConn connectControlDB(LoadConfig config) throws Exception {
        Element control = LoadConfig.getElement(config.getXmlDoc(), "control");
        return new DBConn(
                LoadConfig.getValue(control, "url"),
                LoadConfig.getValue(control, "username"),
                LoadConfig.getValue(control, "password")
        );
    }

    private static DBConn connectStagingDB(LoadConfig config) throws Exception {
        Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
        Element staging = LoadConfig.getChildElement(database, "staging");
        return new DBConn(
                LoadConfig.getValue(staging, "url"),
                LoadConfig.getValue(staging, "username"),
                LoadConfig.getValue(staging, "password")
        );
    }
}