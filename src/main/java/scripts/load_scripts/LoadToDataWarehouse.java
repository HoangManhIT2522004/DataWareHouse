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

    // ✅ STATIC BLOCK FOR DEBUG
    static {
        System.out.println("=== LoadToDataWarehouse CLASS LOADED ===");
        System.out.println("Java Version: " + System.getProperty("java.version"));
        System.out.println("Working Directory: " + System.getProperty("user.dir"));
        System.out.println("Classpath: " + System.getProperty("java.class.path"));
    }

    /**
     * ============================================================
     * Step 1: Load configuration from XML file
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
            String body = "Unable to load configuration file from path: " + path +
                    "\n\nPlease verify:\n" +
                    "- Does the file exist?\n" +
                    "- Is the path correct?\n" +
                    "- Does the file have read permissions?\n" +
                    "- Is the XML structure valid?";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 1] FAILED: Cannot load config file");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 2: Connect to Control Database
     * ============================================================
     */
    public static DBConn connectControlDB(LoadConfig config) {
        System.out.println("[Step 2] Connecting to Control database...");
        try {
            Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
            Element control = LoadConfig.getChildElement(database, "control");

            System.out.println("[DEBUG] Database element: " + (database != null ? "Found" : "NULL"));
            System.out.println("[DEBUG] Control element: " + (control != null ? "Found" : "NULL"));

            String url = LoadConfig.getValue(control, "url");
            String username = LoadConfig.getValue(control, "username");
            String password = LoadConfig.getValue(control, "password");

            System.out.println("[DEBUG] URL: " + url);
            System.out.println("[DEBUG] Username: " + username);
            System.out.println("[DEBUG] Password: " + (password.isEmpty() ? "EMPTY!" : "***"));

            DBConn db = new DBConn(url, username, password);

            // Test connection
            db.executeQuery("SELECT 1", rs -> {
                if (rs.next()) {
                    System.out.println("[Step 2] Control database connection successful");
                }
            });

            return db;

        } catch (Exception e) {
            String subject = "ERROR: Control Database Connection Failed";
            String body = "Unable to connect to Control database.\n\n" +
                    "Please verify:\n" +
                    "- Is the database server running?\n" +
                    "- Are the connection details correct?\n" +
                    "- Is the network connection available?";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 2] FAILED: Cannot connect to Control database");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 3: Connect to Staging Database
     * ============================================================
     */
    public static DBConn connectStagingDB(LoadConfig config) {
        System.out.println("[Step 3] Connecting to Staging database...");
        try {
            Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
            Element staging = LoadConfig.getChildElement(database, "staging");

            System.out.println("[DEBUG] Staging element: " + (staging != null ? "Found" : "NULL"));

            String url = LoadConfig.getValue(staging, "url");
            String username = LoadConfig.getValue(staging, "username");
            String password = LoadConfig.getValue(staging, "password");

            System.out.println("[DEBUG] URL: " + url);
            System.out.println("[DEBUG] Username: " + username);
            System.out.println("[DEBUG] Password: " + (password.isEmpty() ? "EMPTY!" : "***"));

            DBConn db = new DBConn(url, username, password);

            // Test connection
            db.executeQuery("SELECT 1", rs -> {
                if (rs.next()) {
                    System.out.println("[Step 3] Staging database connection successful");
                }
            });

            return db;

        } catch (Exception e) {
            String subject = "ERROR: Staging Database Connection Failed";
            String body = "Unable to connect to Staging database.\n\n" +
                    "Please verify:\n" +
                    "- Is the database server running?\n" +
                    "- Are the connection details correct?\n" +
                    "- Is the network connection available?";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 3] FAILED: Cannot connect to Staging database");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 4: Connect to Warehouse Database
     * ============================================================
     */
    public static DBConn connectWarehouseDB(LoadConfig config) {
        System.out.println("[Step 4] Connecting to Warehouse database...");
        try {
            Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
            Element warehouse = LoadConfig.getChildElement(database, "warehouse");

            System.out.println("[DEBUG] Warehouse element: " + (warehouse != null ? "Found" : "NULL"));

            String url = LoadConfig.getValue(warehouse, "url");
            String username = LoadConfig.getValue(warehouse, "username");
            String password = LoadConfig.getValue(warehouse, "password");

            System.out.println("[DEBUG] URL: " + url);
            System.out.println("[DEBUG] Username: " + username);
            System.out.println("[DEBUG] Password: " + (password.isEmpty() ? "EMPTY!" : "***"));

            DBConn db = new DBConn(url, username, password);

            // Test connection
            db.executeQuery("SELECT 1", rs -> {
                if (rs.next()) {
                    System.out.println("[Step 4] Warehouse database connection successful");
                }
            });

            return db;

        } catch (Exception e) {
            String subject = "ERROR: Warehouse Database Connection Failed";
            String body = "Unable to connect to Warehouse database.\n\n" +
                    "Please verify:\n" +
                    "- Is the database server running?\n" +
                    "- Are the connection details correct?\n" +
                    "- Is the network connection available?";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 4] FAILED: Cannot connect to Warehouse database");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 5: Check if today's warehouse load already succeeded
     * ============================================================
     */
    public static void checkTodayWarehouseLoadSuccess() {
        try {
            System.out.println("[Step 5] Checking today's warehouse load status...");
            String sql = "SELECT check_today_success_loadwarehouse(?) AS success";
            final boolean[] alreadyDone = {false};

            controlDB.executeQuery(sql, rs -> {
                if (rs.next()) {
                    alreadyDone[0] = rs.getBoolean("success");
                }
            }, "LOD_DW");

            if (alreadyDone[0]) {
                String subject = "Weather ETL - Warehouse Load Already Completed Today";
                String body = "The system detected that warehouse load has already been completed successfully today.\n\n" +
                        "Time: " + LocalDateTime.now().format(dtf) +
                        "\n\nStopping process to avoid duplicates.";
                EmailSender.sendEmail(subject, body);
                System.out.println("[Step 5] Warehouse load already completed today, stopping process.");
                System.exit(0);
            } else {
                System.out.println("[Step 5] No warehouse load success found today, proceeding...");
            }

        } catch (Exception e) {
            String subject = "ERROR: Check Today Warehouse Load Failed";
            String body = "Error occurred while checking today's warehouse load status";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 5] FAILED: Error checking today's warehouse load");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * ============================================================
     * Step 6: Prepare warehouse load process
     * ============================================================
     */
    public static String prepareWarehouseLoadProcess() {
        try {
            System.out.println("[Step 6] Preparing warehouse load process...");

            String processName = "LOD_DW_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

            System.out.println("[Step 6] Process info:");
            System.out.println("  - Process Name  : " + processName);
            System.out.println("  - Process Type  : load_warehouse");
            System.out.println("  - Source        : staging_to_warehouse");
            System.out.println("  - Destination   : warehouse");
            System.out.println("  - Description   : dim_and_fact_tables");

            // Step 1: Get or create config_process
            String sqlConfig = String.format(
                    "SELECT get_or_create_process_loadwarehouse('%s', 'load_warehouse', 'staging_to_warehouse', 'warehouse', 'dim_and_fact_tables')",
                    processName
            );

            final int[] configId = {0};
            controlDB.executeQuery(sqlConfig, rs -> {
                if (rs.next()) {
                    configId[0] = rs.getInt(1);
                }
            });

            if (configId[0] == 0) {
                throw new Exception("Cannot get or create config_process");
            }

            System.out.println("[Step 6] Config Process ID: " + configId[0]);

            // Step 2: Create execution log
            String sqlLog = "SELECT create_new_log_loadwarehouse(" + configId[0] + ")";
            final String[] execId = {null};
            controlDB.executeQuery(sqlLog, rs -> {
                if (rs.next()) {
                    execId[0] = rs.getString(1);
                }
            });

            if (execId[0] == null) {
                throw new Exception("Cannot create execution log");
            }

            System.out.println("[Step 6] Created execution: " + execId[0]);
            return execId[0];

        } catch (Exception e) {
            String subject = "ERROR: Prepare Warehouse Load Failed";
            String body = "Error occurred while preparing warehouse load process";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 6] FAILED: Cannot prepare warehouse load");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Update process log status
     * ============================================================
     */
    private static void updateProcessLogStatus(String execId, String status, int inserted, int failed, String message) {
        try {
            String sql = "SELECT update_log_status_loadwarehouse(?, ?::process_status, ?, ?, ?)";
            controlDB.executeQuery(sql, rs -> {}, execId, status, inserted, failed, message);
            System.out.println("[Log] Status updated: " + status);
        } catch (Exception e) {
            System.err.println("Cannot update log: " + e.getMessage());
        }
    }

    /**
     * ============================================================
     * Send success email
     * ============================================================
     */
    private static void sendSuccessEmail(int total) {
        String subject = "Weather ETL - LOAD TO DATA WAREHOUSE SUCCESSFUL";

        String body = """
                ======================================
                   WEATHER ETL - LOAD TO WAREHOUSE
                          SUCCESSFUL
                ======================================

                Execution ID : %s
                Time         : %s
                Total Records: %,d records

                System is running smoothly.
                """.formatted(
                executionId,
                LocalDateTime.now().format(dtf),
                total
        );

        EmailSender.sendEmail(subject, body);
        System.out.println("[Email] Success email sent!");
    }

    /**
     * ============================================================
     * Send error email
     * ============================================================
     */
    private static void sendErrorEmail(Exception e) {
        String subject = "Weather ETL - LOAD TO WAREHOUSE FAILED";

        String body = """
                ======================================
                   WEATHER ETL - LOAD TO WAREHOUSE
                              FAILED
                ======================================

                Execution ID : %s
                Time         : %s

                Error:
                %s

                Immediate attention required.
                """
                .formatted(
                        executionId != null ? executionId : "N/A",
                        LocalDateTime.now().format(dtf),
                        e.getMessage()
                );

        EmailSender.sendError(subject, body, e);
        System.out.println("[Email] Error email sent!");
    }

    /**
     * ============================================================
     * Step 7: Load data to warehouse
     * ============================================================
     */
    private static int loadToWarehouse(String execId) throws Exception {
        System.out.println("[Step 7] Starting Load to Warehouse...");
        int total = 0;

        try (Connection connStaging = stagingDB.getConnection();
             Connection connWarehouse = warehouseDB.getConnection()) {

            connWarehouse.setAutoCommit(false);

            // 1. DIMENSION TABLES: Load/Merge Incremental
            total += loadDimensionIncremental(connWarehouse, connStaging, "dim_location", "location_id");
            total += loadDimensionIncremental(connWarehouse, connStaging, "dim_weather_condition", "condition_id");

            // 2. FACT TABLES: Load today's data only (Incremental/Daily Refresh)
            total += loadFactDailyIncremental(connWarehouse, connStaging, "fact_weather_daily", "observation_date");
            total += loadFactDailyIncremental(connWarehouse, connStaging, "fact_air_quality_daily", "observation_time");

            connWarehouse.commit();
            System.out.println("[Step 7] Warehouse load completed. Total records: " + total);
        }
        return total;
    }

    /**
     * ============================================================
     * Load fact table with daily incremental strategy
     * ============================================================
     */
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

        // Delete today's data in warehouse (to handle re-run)
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

        System.out.println(totalRows + " rows.");
        return totalRows;
    }

    /**
     * ============================================================
     * Load dimension table with incremental strategy
     * ============================================================
     */
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

    /**
     * ============================================================
     * Get column list from table
     * ============================================================
     */
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

    /**
     * ============================================================
     * Print usage instructions
     * ============================================================
     */
    private static void printUsage() {
        System.out.println("Usage: java LoadToDataWarehouse <config_path>");
        System.out.println();
        System.out.println("Arguments:");
        System.out.println("  config_path : Path to configuration file (e.g., config/config.xml)");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java LoadToDataWarehouse config/config.xml");
    }

    /**
     * ============================================================
     * MAIN METHOD
     * ============================================================
     */
    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("   WEATHER ETL - STEP 4: LOAD TO WAREHOUSE");
        System.out.println("========================================\n");

        // Check arguments
        if (args.length < 1) {
            System.err.println("ERROR: Missing required arguments!");
            System.err.println();
            printUsage();
            System.exit(1);
        }

        String configPath = args[0];

        System.out.println("Configuration:");
        System.out.println("  - Config File : " + configPath);
        System.out.println();

        try {
            // Step 1: Load config
            LoadConfig config = loadConfig(configPath);

            // Step 2-4: Connect to databases
            controlDB = connectControlDB(config);
            stagingDB = connectStagingDB(config);
            warehouseDB = connectWarehouseDB(config);

            // Step 5: Check if today's load already succeeded
            checkTodayWarehouseLoadSuccess();

            // Step 6: Prepare warehouse load process
            executionId = prepareWarehouseLoadProcess();

            // Step 7: Load data to warehouse
            int total = loadToWarehouse(executionId);

            // Step 8: Update log status
            updateProcessLogStatus(executionId, "success", total, 0,
                    "Load to Data Warehouse successfully");

            System.out.println("\nTotal records loaded: " + total);

            // Step 9: Send success email
            sendSuccessEmail(total);

            System.out.println("\n========================================");
            System.out.println("LOAD TO DATA WAREHOUSE COMPLETED SUCCESSFULLY");
            System.out.println("========================================");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("\n========================================");
            System.err.println("LOAD TO DATA WAREHOUSE FAILED");
            System.err.println("========================================");
            e.printStackTrace();

            if (executionId != null)
                updateProcessLogStatus(executionId, "failed", 0, 0, e.getMessage());

            sendErrorEmail(e);
            System.exit(1);
        }
    }
}