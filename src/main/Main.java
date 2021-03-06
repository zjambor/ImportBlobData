package main;

import java.nio.file.*;
import java.sql.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static String userHome = System.getProperty("user.home");

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
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

        var path = Paths.get(userHome + "/blob_conf2.txt");
        try {
            connectionstring = Files.readString(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //content = id + "|" + url + "|" + CREATE_USER_ID + "|" + CREATE_USER_DATE + "\n";
        path = Paths.get(userHome + "/photos.txt");
        try {
            var list = Files.readAllLines(path);
            list.forEach(line -> {
                String[] l = line.split("[|]");
                ids.add(Integer.parseInt(l[0]));
                urls.add(l[1]);
                CREATE_USER_IDs.add(Integer.parseInt(l[2]));
                CREATE_USER_DATEs.add(Long.parseLong(l[3]));
            });
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        System.out.println(userHome + "/photos.txt loaded, number of records: " + ids.size());

        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection con = DriverManager.getConnection(connectionstring);
        con.setAutoCommit(false);
        PreparedStatement ps = null;
        int i = 0;

        try {
            for (i = 0; i < ids.size(); i++) {
                ps = con.prepareStatement("insert into lksz.fotok (id,url,FOTO,CREATE_USER_ID,CREATE_USER_DATE) values (?,?,?,?,?)");

                ps.setInt(1, ids.get(i));  // set the PK value
                ps.setString(2, urls.get(i));
                ps.setInt(4, CREATE_USER_IDs.get(i));
                ps.setLong(5, CREATE_USER_DATEs.get(i));

                try {
                    var blob = new File("/export/exp/" + urls.get(i));
                    var in = new FileInputStream(blob);

                    ps.setBinaryStream(3, in, (int) blob.length());
                    System.out.println(i + " | File inserted: " + blob.getName());
                } catch (Exception e) {
                    ps.setNull(3, Types.VARCHAR);
                }

                ps.executeUpdate();
                con.commit();

                ps.close();
            }

            con.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            con.rollback();
            ps.close();
            System.out.println(urls.get(i) + " file inserting failed.");
        }
        System.out.println("Computation lasted " + (System.currentTimeMillis() - start) / 1000 / 60 + " minutes.");
    }
}
