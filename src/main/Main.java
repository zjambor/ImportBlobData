package main;

import java.nio.file.*;
import java.sql.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        List<Integer> ids = new ArrayList<>();
        List<String> dates = new ArrayList<>();

        var connectionstring = "";

        var path = Paths.get("d:\\blob_conf2.txt");
        try {
            connectionstring = Files.readString(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        path = Paths.get("d:\\photos.txt");
        try {
            var list = Files.readAllLines(path);
            list.forEach(line -> {
                String[] l = line.split("[|]");
                ids.add(Integer.parseInt(l[0]));
                dates.add(l[1]);
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
                ps.setString(3, dates.get(i));
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
