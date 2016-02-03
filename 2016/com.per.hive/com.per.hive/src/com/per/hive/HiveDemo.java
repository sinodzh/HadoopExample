package com.per.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveDemo {

	static {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		}
	}

	public static void main(String[] args) {
		test();
	}

	/**
	 * @Description : 测试
	 */
	private static void test() {
		try (Connection con = DriverManager
				.getConnection("jdbc:hive2://h188:10000/")) {
			Statement stm = con.createStatement();
			ResultSet rs = stm.executeQuery("select * from score ");
			
			while (rs.next()) {
				String info = rs.getString(1);
				info += " " + rs.getString(2);
				info += " " + rs.getString(3);
				info += " " + rs.getString(4);

				System.out.println(info);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
