import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import org.json.*;
public class ReadCard {
    private static final int BATCH_SIZE = 1000;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private static String filepath = "Data_v1\\cards.json";
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
    public void truncateCard() {
        String sql = "truncate table card cascade";
        try {
            stmt = conn.prepareStatement(sql);
            stmt.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void loadCard(JSONObject jsonObject) {
        try {
            //处理读入的文件
            String code = jsonObject.getString("code");
            double money = jsonObject.getDouble("money");
            Timestamp create_time = Timestamp.valueOf(jsonObject.getString("create_time"));
            //设置sql语句
            stmt.setString(1, code);
            stmt.setDouble(2, money);
            stmt.setTimestamp(3, create_time);
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
    private void insertCard(){
        openDB();
        truncateCard();
        closeDB();

        long start, end;
        start = System.currentTimeMillis();
        openDB();
        try {
            //文件读取与数据处理
            BufferedReader bf = new BufferedReader(new FileReader(filepath));
            String sql = "insert into card values (?, ?, ?)";
            StringBuilder jsonContent = new StringBuilder();
            String line;
            while ((line = bf.readLine()) != null) {
                jsonContent.append(line.trim());
            }
            JSONArray jsonArray = new JSONArray(jsonContent.toString());
            stmt = conn.prepareStatement(sql);
            for (int i = 0; i < jsonArray.length(); i++) {
                loadCard(jsonArray.getJSONObject(i));
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
        ReadCard cr = new ReadCard();
        cr.insertCard();
    }
}
