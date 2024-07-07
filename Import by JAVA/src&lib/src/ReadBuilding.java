import org.json.JSONArray;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

public class ReadBuilding {
    private static final int BATCH_SIZE = 50;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private static String filepath = "Data_v1\\stations.json";
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
    public void truncateBuilding() {
        String sql = "truncate table buildings cascade";
        try {
            stmt = conn.prepareStatement(sql);
            stmt.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void insertBuilding(){
        openDB();
        truncateBuilding();
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
            String sql = "insert into buildings(exit_id, name) values (?, ?)";
            stmt = conn.prepareStatement(sql);
            for (String key : Object.keySet()) {
                JSONObject jsonObject = Object.getJSONObject(key);
                try {
                    JSONArray outInfo = jsonObject.getJSONArray("out_info");
                    if (key.contains("'")){
                        key = key.replace("'","");
                    }
                    String selectQuery1 = "select id from station where english_name = ?";
                    try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery1)) {
                        selectStmt.setString(1, key);
                        ResultSet result = selectStmt.executeQuery();
                        if (result.next()) {
                            for (int i = 0; i < outInfo.length(); i++) {
                                JSONObject out = outInfo.getJSONObject(i);
                                String selectQuery2 = "select id from exit where number = ?";
                                try (PreparedStatement selectStmt2 = conn.prepareStatement(selectQuery2)){
                                    String number = out.getString("outt").replaceAll("[^\\x20-\\x7E\\u4E00-\\u9FA5]", "");
                                    selectStmt2.setString(1, number);
                                    ResultSet result2 = selectStmt2.executeQuery();
                                    if (result2.next()){
                                        int exitId = result2.getInt("id");
                                        String buildings = out.getString("textt");
                                        String[] buildingArr;
                                        if (buildings.contains("、")){
                                            buildingArr = buildings.split("、");
                                        } else {
                                            buildingArr = buildings.split("，");
                                        }
                                        //设置sql语句
                                        for (String building : buildingArr){
                                            stmt.setInt(1, exitId);
                                            stmt.setString(2, building);
                                            stmt.addBatch();
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e){
                        e.printStackTrace();
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
        ReadBuilding rb = new ReadBuilding();
        rb.insertBuilding();
    }
}
