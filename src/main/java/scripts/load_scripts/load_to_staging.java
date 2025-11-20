package scripts.load_scripts;

import utils.DBConn;
import utils.EmailSender; // Import module gửi mail
import utils.LoadConfig;
import org.w3c.dom.Element;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class load_to_staging {

    public static void main(String[] args) {
        // Lấy ngày hiện tại để định vị file CSV
        String dateStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String csvPath = "D:/DataWareHouse/src/main/java/data/weatherapi_" + dateStr + ".csv";

        // Log bắt đầu
        System.out.println("========================================");
        System.out.println("   WEATHER ETL - STEP 6: LOAD TO RAW");
        System.out.println("========================================");

        // 1. KIỂM TRA KẾT QUẢ BƯỚC EXTRACT
        // Nếu file không tồn tại nghĩa là bước ExtractToFile đã thất bại hoặc chưa chạy
        File file = new File(csvPath);
        if (!file.exists()) {
            String msg = "Không tìm thấy file dữ liệu: " + csvPath + ".\nCó thể bước Extract đã thất bại.";
            System.err.println("❌ [ABORT] " + msg);

            // Gửi mail báo động cho Admin
            EmailSender.sendError("ETL Alert: Load To Raw Aborted", msg, new Exception("File Not Found"));
            return; // Dừng chương trình
        }

        System.out.println("✅ [Check] File extract tồn tại: " + csvPath);

        // 2. THỰC HIỆN LOAD
        try {
            // Load cấu hình DB
            LoadConfig config = new LoadConfig("config/config.xml");
            Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
            Element stagingNode = LoadConfig.getChildElement(database, "staging");
            String url = LoadConfig.getValue(stagingNode, "url");
            String user = LoadConfig.getValue(stagingNode, "username");
            String pass = LoadConfig.getValue(stagingNode, "password");

            // Kết nối DB
            DBConn dbConn = new DBConn(url, user, pass);

            // Gọi hàm xử lý chính
            loadToRawTables(file, dbConn);

        } catch (Exception e) {
            // Bắt lỗi cấu hình hoặc kết nối DB ban đầu
            System.err.println("❌ [CRITICAL] Lỗi khởi tạo tiến trình Load.");
            e.printStackTrace();
            EmailSender.sendError("ETL Critical: Load To Raw Init Failed", "Lỗi khởi tạo kết nối hoặc cấu hình.", e);
        }
    }

    public static void loadToRawTables(File file, DBConn dbConn) {
        System.out.println("[Process] Đang đọc file và đẩy vào Database...");

        Connection conn = null;
        // Khai báo PreparedStatement
        PreparedStatement psLoc = null, psCond = null, psAir = null, psObs = null;

        // SQL Statements (Lưu ý: "localtime" đã được escape)
        String sqlLoc = "INSERT INTO raw_weather_location (name, region, country, lat, lon, tz_id, \"localtime\", source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";
        String sqlCond = "INSERT INTO raw_weather_condition (code, text, icon, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?::jsonb)";
        String sqlAir = "INSERT INTO raw_air_quality (co, no2, o3, so2, pm2_5, pm10, us_epa_index, gb_defra_index, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";
        String sqlObs = "INSERT INTO raw_weather_observation (last_updated, temp_c, temp_f, feelslike_c, feelslike_f, humidity, cloud, vis_km, vis_miles, uv, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, location_name, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";

        int count = 0;
        try {
            conn = dbConn.getConnection();
            conn.setAutoCommit(false); // Tắt auto-commit để quản lý transaction

            // Chuẩn bị câu lệnh
            psLoc = conn.prepareStatement(sqlLoc);
            psCond = conn.prepareStatement(sqlCond);
            psAir = conn.prepareStatement(sqlAir);
            psObs = conn.prepareStatement(sqlObs);

            BufferedReader br = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8));
            String line;
            String[] headers = null;
            Map<String, Integer> map = new HashMap<>();

            // Xử lý Header
            if ((line = br.readLine()) != null) {
                if (line.startsWith("\uFEFF")) line = line.substring(1); // Xóa BOM
                headers = line.split(",");
                for (int i = 0; i < headers.length; i++) {
                    map.put(headers[i].trim(), i);
                }
            }

            Gson gson = new Gson(); // Dùng để tạo JSON payload

            while ((line = br.readLine()) != null) {
                try {
                    // Tách CSV thông minh (xử lý dấu phẩy trong chuỗi)
                    String[] row = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                    for(int i=0; i<row.length; i++) row[i] = row[i].replaceAll("^\"|\"$", "");

                    // Lấy dữ liệu chung
                    String batchId = row[map.get("execution_id")];
                    String source = "WeatherAPI";

                    // Tạo JSON full row
                    JsonObject json = new JsonObject();
                    for(String h : map.keySet()) {
                        if(map.get(h) < row.length) json.addProperty(h, row[map.get(h)]);
                    }
                    String jsonStr = gson.toJson(json);

                    // --- 1. LOCATION ---
                    psLoc.setString(1, row[map.get("location_name")]);
                    psLoc.setString(2, row[map.get("region")]);
                    psLoc.setString(3, "Vietnam"); // Default
                    psLoc.setObject(4, null); // lat
                    psLoc.setObject(5, null); // lon
                    psLoc.setObject(6, null); // tz
                    psLoc.setString(7, row[map.get("last_updated")]); // localtime
                    psLoc.setString(8, source);
                    psLoc.setString(9, batchId);
                    psLoc.setString(10, jsonStr);
                    psLoc.addBatch();

                    // --- 2. CONDITION ---
                    psCond.setString(1, row[map.get("condition_code")]);
                    psCond.setString(2, row[map.get("condition_text")]);
                    psCond.setObject(3, null); // icon
                    psCond.setString(4, source);
                    psCond.setString(5, batchId);
                    psCond.setString(6, jsonStr);
                    psCond.addBatch();

                    // --- 3. AIR QUALITY ---
                    psAir.setString(1, row[map.get("co")]);
                    psAir.setString(2, row[map.get("no2")]);
                    psAir.setString(3, row[map.get("o3")]);
                    psAir.setString(4, row[map.get("so2")]);
                    psAir.setString(5, row[map.get("pm2_5")]);
                    psAir.setString(6, row[map.get("pm10")]);
                    psAir.setString(7, row[map.get("aqi_us")]);
                    psAir.setString(8, row[map.get("aqi_gb")]);
                    psAir.setString(9, source);
                    psAir.setString(10, batchId);
                    psAir.setString(11, jsonStr);
                    psAir.addBatch();

                    // --- 4. OBSERVATION ---
                    psObs.setString(1, row[map.get("last_updated")]);
                    psObs.setString(2, row[map.get("temp_c")]);
                    psObs.setString(3, row[map.get("temp_f")]);
                    psObs.setString(4, row[map.get("feels_like_c")]);
                    psObs.setString(5, row[map.get("feels_like_f")]);
                    psObs.setString(6, row[map.get("humidity")]);
                    psObs.setString(7, row[map.get("cloud")]);
                    psObs.setString(8, row[map.get("vis_km")]);
                    psObs.setString(9, row[map.get("vis_miles")]);
                    psObs.setString(10, row[map.get("uv")]);
                    psObs.setString(11, row[map.get("wind_mph")]);
                    psObs.setString(12, row[map.get("wind_kph")]);
                    psObs.setString(13, row[map.get("wind_degree")]);
                    psObs.setString(14, row[map.get("wind_dir")]);
                    psObs.setString(15, row[map.get("pressure_mb")]);
                    psObs.setString(16, row[map.get("pressure_in")]);
                    psObs.setString(17, row[map.get("precip_mm")]);
                    psObs.setString(18, row[map.get("precip_in")]);
                    psObs.setString(19, row[map.get("location_name")]);
                    psObs.setString(20, source);
                    psObs.setString(21, batchId);
                    psObs.setString(22, jsonStr);
                    psObs.addBatch();

                    count++;
                    // Thực thi batch mỗi 100 dòng để tối ưu bộ nhớ
                    if (count % 100 == 0) {
                        psLoc.executeBatch();
                        psCond.executeBatch();
                        psAir.executeBatch();
                        psObs.executeBatch();
                    }
                } catch (Exception e) {
                    // Lỗi dòng dữ liệu cụ thể (không dừng chương trình, chỉ log)
                    System.err.println("  ⚠️ [Skip] Lỗi xử lý dòng CSV: " + e.getMessage());
                }
            }

            // Thực thi batch những dòng còn lại
            psLoc.executeBatch();
            psCond.executeBatch();
            psAir.executeBatch();
            psObs.executeBatch();

            conn.commit(); // CHỐT GIAO DỊCH

            System.out.println("✅ [Success] Load to RAW completed.");
            System.out.println("   - Tổng số dòng đã load: " + count);

        } catch (SQLException | java.io.IOException e) {
            // ROLLBACK NẾU CÓ LỖI
            try { if(conn != null) conn.rollback(); } catch (SQLException ex) {}

            String errMsg = "Lỗi trong quá trình Insert vào Database.\nChi tiết: " + e.getMessage();
            System.err.println("❌ [FAILED] " + errMsg);
            e.printStackTrace();

            // GỬI EMAIL BÁO LỖI
            EmailSender.sendError("ETL Error: Load To Raw Failed", errMsg, (Exception) e);

        } finally {
            // Đóng kết nối an toàn
            try { if(psLoc != null) psLoc.close(); } catch (SQLException ex) {}
            try { if(psCond != null) psCond.close(); } catch (SQLException ex) {}
            try { if(psAir != null) psAir.close(); } catch (SQLException ex) {}
            try { if(psObs != null) psObs.close(); } catch (SQLException ex) {}
            try { if(conn != null) conn.close(); } catch (SQLException ex) {}
        }
    }
}
