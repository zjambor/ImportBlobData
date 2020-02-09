package main;

import java.nio.file.*;
import java.sql.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        var id = 0;
        var CREATE_USER_ID = 0;
        long CREATE_USER_DATE = 0;
        var url = "";

        List<Integer> ids = new ArrayList<>();
        List<String> urls = new ArrayList<>();
        List<Integer> CREATE_USER_IDs = new ArrayList<>();
        List<Long> CREATE_USER_DATEs = new ArrayList<>();

        var connectionstring = "";

        var path = Paths.get("d:\\blob_conf2.txt");
        try {
            connectionstring = Files.readString(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //content = id + "|" + url + "|" + CREATE_USER_ID + "|" + CREATE_USER_DATE + "\n";
        path = Paths.get("d:\\photos.txt");
        try {
            var list = Files.readAllLines(path);
            list.forEach(line -> {
                String[] l = line.split("[|]");
                ids.add(Integer.parseInt(l[0]));
                urls.add(l[1]);
                CREATE_USER_IDs.add(Integer.parseInt(l[2]));
                CREATE_USER_DATEs.add(Long.parseLong(l[2]));
            });
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        System.out.println("d:\\photos.txt loaded, number of records: " + ids.size());

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = DriverManager.getConnection(connectionstring);
            con.setAutoCommit(false);

            for (int i = 0; i < ids.size(); i++) {
                PreparedStatement ps =
                        con.prepareStatement("insert into BLOBTAB3 (blob_data,id,createdate) values (?,?,?)");
                var blob = new File("D:\\photos\\" + ids.get(i));
                var in = new FileInputStream(blob);

                ps.setBinaryStream(1, in, (int) blob.length());

                ps.setInt(2, ids.get(i));  // set the PK value
                ps.setString(3, urls.get(i));
                ps.executeUpdate();
                con.commit();

                ps.close();
                System.out.println("File inserted: " + blob.getName());
            }

            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Computation lasted " + (System.currentTimeMillis() - start) + " milliseconds.");
    }
}
