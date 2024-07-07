import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.text.SimpleDateFormat;
import org.json.*;
public class ReadLine {
    private static final int BATCH_SIZE = 5;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private static String filepath = "Data_v1\\lines.json";
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
    public void truncateLine() {
        String sql = "truncate table line cascade";
        try {
            stmt = conn.prepareStatement(sql);
            stmt.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void insertLine(){
        openDB();
        truncateLine();
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
            JSONObject Object = new JSONObject(jsonContent.toString());
            String sql = "insert into line values (?, ?, ?, ?, ?, ?, ?, ?)";
            stmt = conn.prepareStatement(sql);
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
            for (String key : Object.keySet()) {
                JSONObject jsonObject = Object.getJSONObject(key);
                try {
                    //处理读入的文件
                    Time startTime = new Time(sdf.parse(jsonObject.getString("start_time")).getTime());
                    Time endTime = new Time(sdf.parse(jsonObject.getString("end_time")).getTime());
                    String intro = jsonObject.getString("intro");
                    double mileage = jsonObject.getDouble("mileage");
                    String color = jsonObject.getString("color");
                    java.sql.Date firstOpening = java.sql.Date.valueOf(jsonObject.getString("first_opening"));
                    String url = jsonObject.getString("url");
                    //设置sql语句
                    stmt.setString(1, key);
                    stmt.setTime(2, startTime);
                    stmt.setTime(3, endTime);
                    stmt.setString(4, intro);
                    stmt.setDouble(5, mileage);
                    stmt.setString(6, color);
                    stmt.setDate(7, firstOpening);
                    stmt.setString(8, url);
                    stmt.addBatch();
                    if (++count % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                } catch (Exception e) {
                    System.out.println(jsonObject.toString());
                    e.printStackTrace();
                }

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
        ReadLine rl = new ReadLine();
        rl.insertLine();
    }
}
