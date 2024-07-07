import org.json.JSONArray;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
public class ReadBusLine {
    private static final int BATCH_SIZE = 30;
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
    public void truncateBusLine() {
        String sql = "truncate table bus_line cascade";
        try {
            stmt = conn.prepareStatement(sql);
            stmt.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void insertBusLine(){
        openDB();
        truncateBusLine();
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
            String sql = "insert into bus_line values (?, ?) on conflict do nothing";
            String checkQuery = "select count(*) from bus_line where bus_stop_id = ? and name = ?";//后面筛选是否重复插入的语句靠这个
            stmt = conn.prepareStatement(sql);
            for (String key : Object.keySet()) {
                JSONObject jsonObject = Object.getJSONObject(key);
                try {
                    JSONArray busInfo = jsonObject.getJSONArray("bus_info");
                    if (key.contains("'")){
                        key = key.replace("'","");
                    }
                    String selectQuery1 = "select id from station where english_name = ?";
                    try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery1)) {
                        selectStmt.setString(1, key);
                        ResultSet result = selectStmt.executeQuery();
                        if (result.next()) {
                            for (int i = 0; i < busInfo.length(); i++) {
                                JSONObject busOut = busInfo.getJSONObject(i);
                                String selectQuery2 = "select id from exit where number = ?";
                                try (PreparedStatement selectStmt2 = conn.prepareStatement(selectQuery2)){
                                    String number = busOut.getString("chukou").replaceAll("[^\\x20-\\x7E\\u4E00-\\u9FA5]", "");
                                    selectStmt2.setString(1, number);
                                    JSONArray busOutInfo = busOut.getJSONArray("busOutInfo");
                                    ResultSet result2 = selectStmt2.executeQuery();
                                    if (result2.next()){
                                        int exitId = result2.getInt("id");
                                        for (int j = 0; j < busOutInfo.length(); j++) {
                                            String busStop = busOutInfo.getJSONObject(j).getString("busName");
                                            String BusLine = busOutInfo.getJSONObject(j).getString("busInfo");
                                            String[] busLines;
                                            if (BusLine.contains("、")){
                                                busLines = BusLine.split("、");
                                            } else {
                                                busLines = BusLine.split(",");
                                            }
                                            String selectQuery3 = "select id from bus_stop where name = ?";
                                            try (PreparedStatement selectStmt3 = conn.prepareStatement(selectQuery3)){
                                                selectStmt3.setString(1,busStop);
                                                ResultSet result3 = selectStmt3.executeQuery();
                                                if(result3.next()) {
                                                    int busStopId = result3.getInt("id");
                                                    //设置sql语句，主要就是改这块
                                                    for (String busLine : busLines){
                                                        try (PreparedStatement stmt1 = conn.prepareStatement(checkQuery)) {
                                                            stmt1.setInt(1,busStopId);
                                                            stmt1.setString(2, busLine);
                                                            ResultSet rs = stmt1.executeQuery();
                                                            rs.next();
                                                            int cnt = rs.getInt("count");
                                                            if (cnt == 0) {//判断是否插入
                                                                stmt.setInt(1, busStopId);
                                                                stmt.setString(2, busLine);
                                                                stmt.addBatch();
                                                            }
                                                        }
                                                    }
                                                }
                                            }
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
        ReadBusLine rbl = new ReadBusLine();
        rbl.insertBusLine();
    }
}
