import org.json.JSONArray;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
public class ReadLineStation {
    private static final int BATCH_SIZE = 20;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private static String filepath2 = "Data_v1\\lines.json";
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
    public void truncateLineStation() {
        String sql = "truncate table line_detail cascade";
        try {
            stmt = conn.prepareStatement(sql);
            stmt.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void insertLineStation(){
        openDB();
        truncateLineStation();
        closeDB();
        long start, end;
        start = System.currentTimeMillis();
        openDB();
        try {
            //文件读取与数据处理
            BufferedReader bf2 = new BufferedReader(new FileReader(filepath2));
            StringBuilder jsonContent2 = new StringBuilder();
            String line2;
            while ((line2 = bf2.readLine()) != null) {
                jsonContent2.append(line2.trim());
            }
            JSONObject line = new JSONObject(jsonContent2.toString());
            String sql = "insert into line_detail values (?, ?)";
            stmt = conn.prepareStatement(sql);
            for (String key : line.keySet()) {
                JSONObject jsonObject = line.getJSONObject(key);
                try {
                    //数据处理
                    JSONArray stations = jsonObject.getJSONArray("stations");
                    //设置sql语句
                    for (int i = 0 ; i < stations.length() ; i ++){
                        stmt.setString(1, key);
                        String station = stations.getString(i);
                        if (station.contains("'")){
                            station = station.replace("'","");
                        }
                        stmt.setString(2, station.trim());
                        stmt.addBatch();
                    }
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
        ReadLineStation rls = new ReadLineStation();
        rls.insertLineStation();
    }
}
