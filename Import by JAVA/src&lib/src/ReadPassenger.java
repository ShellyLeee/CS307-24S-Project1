import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import org.json.*;
public class ReadPassenger {
    private static final int BATCH_SIZE = 1000;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private static String filepath = "Data_v1\\passenger.json";
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
    public void truncatePassenger() {
        String sql = "truncate table passenger cascade";
        try {
            stmt = conn.prepareStatement(sql);
            stmt.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void loadPassenger(JSONObject jsonObject) {
        try {
            //处理读入的文件
            String id = jsonObject.getString("id_number");
            String name = jsonObject.getString("name");
            long phoneNumber = jsonObject.getLong("phone_number");
            String gender = jsonObject.getString("gender");
            String district = jsonObject.getString("district");
            //设置sql语句
            stmt.setString(1, id);
            stmt.setString(2, name);
            stmt.setLong(3, phoneNumber);
            stmt.setString(4, gender);
            stmt.setString(5, district);
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
    private void insertPassenger(){
        openDB();
        truncatePassenger();
        closeDB();

        long start, end;
        start = System.currentTimeMillis();
        openDB();
        try {
            //文件读取与数据处理
            BufferedReader bf = new BufferedReader(new FileReader(filepath));
            String sql = "insert into passenger values (?, ?, ?, ?, ?)";
            StringBuilder jsonContent = new StringBuilder();
            String line;
            while ((line = bf.readLine()) != null) {
                jsonContent.append(line.trim());
            }
            JSONArray jsonArray = new JSONArray(jsonContent.toString());
            stmt = conn.prepareStatement(sql);
            for (int i = 0; i < jsonArray.length(); i++) {
                loadPassenger(jsonArray.getJSONObject(i));
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
        ReadPassenger rp = new ReadPassenger();
        rp.insertPassenger();
    }
}
