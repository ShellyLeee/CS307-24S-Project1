import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import org.json.*;
public class ReadRide {
    private static final int BATCH_SIZE = 1;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private static String filepath = "Data_v1\\ride.json";
    private int count = 0;
    public void openDB() {
        String host = "localhost";
        String port = "5432";
        String db_name = "project1";
        String user = "postgres";
        String pw = "20040509";

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + db_name;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, user, pw);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void closeDB() {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public void truncateRide() {
        String sql = "truncate table card_ride cascade; truncate table passenger_ride cascade";
        try {
            stmt = conn.prepareStatement(sql);
            stmt.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void loadRide(JSONObject jsonObject) {
        try {
            //处理读入的文件
            String code = jsonObject.getString("user");
            String startStation = jsonObject.getString("start_station");
            String endStation = jsonObject.getString("end_station");
            int price = jsonObject.getInt("price");
            Timestamp startTime = Timestamp.valueOf(jsonObject.getString("start_time"));
            Timestamp endTime = Timestamp.valueOf(jsonObject.getString("end_time"));
            if (startStation.contains("'")){
                startStation = startStation.replace("'","");
            }
            if (endStation.contains("'")){
                endStation = endStation.replace("'","");
            }
            String sql;
            if (code.length() == 9){
                sql = "insert into card_ride (card_code, start_station, end_station, price, start_time, end_time) values (?, ?, ?, ?, ?, ?)";
            } else {
                sql = "insert into passenger_ride (passenger_id, start_station, end_station, price, start_time, end_time) values (?, ?, ?, ?, ?, ?)";
            }
            //设置sql语句
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, code);
            stmt.setString(2, startStation.trim());
            stmt.setString(3, endStation.trim());
            stmt.setInt(4, price);
            stmt.setTimestamp(5, startTime);
            stmt.setTimestamp(6, endTime);
            stmt.addBatch();
            if (++count % BATCH_SIZE == 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
        } catch (Exception e) {
            System.out.println(jsonObject);
            e.printStackTrace();
        }
    }
    private void insertRide(){
        openDB();
        truncateRide();
        closeDB();

        long start, end;
        start = System.currentTimeMillis();
        openDB();
        try {
            //文件读取与数据处理
            BufferedReader bf = new BufferedReader(new FileReader(filepath));
            StringBuilder jsonContent = new StringBuilder();

            String line;
            while ((line = bf.readLine()) != null) {
                jsonContent.append(line.trim());
            }
            JSONArray jsonArray = new JSONArray(jsonContent.toString());
            for (int i = 0; i < jsonArray.length(); i++) {
                loadRide(jsonArray.getJSONObject(i));
            }
            stmt.executeBatch();
            stmt.clearBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            closeDB();
        }
        closeDB();
        end = System.currentTimeMillis();
        System.out.println("Insertion time: " + (end - start) + "ms");
    }
    public static void main(String[] args) {
        ReadRide rr = new ReadRide();
        rr.insertRide();
    }
}
