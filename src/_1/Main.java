package _1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class Main {

	public static void main(String[] args) {

		final String url = "jdbc:mysql://localhost";
		final String db = "POBLACION";

		Properties props = new Properties();
		props.put("user", "root");
		props.put("password", "root");

		try {
			Connection connection = DriverManager.getConnection(url, props);

			Statement statement = connection.createStatement();

			statement.executeUpdate("DROP DATABASE IF EXISTS " + db);
			System.out.println("DROP DATABASE");

			statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + db);
			System.out.println("CREATE DATABASE");

			Connection connectionDatabase = DriverManager.getConnection(url + "/" + db, props);
			statement = connectionDatabase.createStatement();

			statement.executeUpdate("CREATE TABLE IF NOT EXISTS PAIS (\r\n" + "CONTINENTE VARCHAR(10) NOT NULL,\r\n"
					+ "NOMBRE VARCHAR(40) NOT NULL,\r\n" + "SUPERFICIE INT NOT NULL,\r\n" + "PRIMARY KEY(NOMBRE)\r\n"
					+ ")");
			System.out.println("CREATE TABLE PAIS");

			statement.executeUpdate("CREATE TABLE IF NOT EXISTS POBLACION (\r\n" + "NOMBRE VARCHAR(40) NOT NULL,\r\n"
					+ "POBLACION INT NOT NULL,\r\n" + "POBLACIONHOMBRES INT NOT NULL,\r\n"
					+ "POBLACIONMUJERES INT NOT NULL,\r\n" + "PRIMARY KEY(NOMBRE)\r\n" + ")\r\n" + "");
			System.out.println("CREATE TABLE POBLACION");

			statement.executeUpdate("CREATE TABLE IF NOT EXISTS POBLACIONHOMBRES (\r\n"
					+ "NOMBRE VARCHAR(40) NOT NULL,\r\n" + "POBLACION_0_4 INT NOT NULL,\r\n"
					+ "POBLACION_5_9 INT NOT NULL,\r\n" + "POBLACION_10_17 INT NOT NULL,\r\n"
					+ "POBLACION_18_29 INT NOT NULL,\r\n" + "POBLACION_30_59 INT NOT NULL,\r\n"
					+ "POBLACION_59 INT NOT NULL,\r\n" + "PRIMARY KEY(NOMBRE)\r\n" + ")\r\n" + "");
			System.out.println("CREATE TABLE POBLACION HOMBRES");

			statement.executeUpdate("CREATE TABLE IF NOT EXISTS POBLACIONMUJERES (\r\n"
					+ "NOMBRE VARCHAR(40) NOT NULL,\r\n" + "POBLACION_0_4 INT NOT NULL,\r\n"
					+ "POBLACION_5_9 INT NOT NULL,\r\n" + "POBLACION_10_17 INT NOT NULL,\r\n"
					+ "POBLACION_18_29 INT NOT NULL,\r\n" + "POBLACION_30_59 INT NOT NULL,\r\n"
					+ "POBLACION_59 INT NOT NULL,\r\n" + "PRIMARY KEY(NOMBRE)\r\n" + ")\r\n" + "");
			System.out.println("CREATE TABLE POBLACION MUJERES");

			statement.executeUpdate("CREATE TABLE IF NOT EXISTS PORCENTAJES (\r\n"
					+ "CONTINENTE VARCHAR(10) NOT NULL,\r\n" + "NOMBRE VARCHAR(40) NOT NULL,\r\n"
					+ "PORCENTAJEHOMBRES DECIMAL(5,2) ,\r\n" + "PORCENTAJEMUJERES DECIMAL(5,2) ,\r\n"
					+ "DENSIDADTOTAL DECIMAL(7,2) ,\r\n" + "DENSIDADHOMBRES DECIMAL(7,2) ,\r\n"
					+ "DENSIDADMUJERES DECIMAL(7,2) ,\r\n" + "PORCENTAJE_18_29HOMBRES DECIMAL(5,2) ,\r\n"
					+ "PORCENTAJE_18_29MUJERES DECIMAL(5,2) ,\r\n" + "PORCENTAJEVIEJOS DECIMAL(5,2) ,\r\n"
					+ "PORCENTAJEJOVENES DECIMAL(5,2) ,\r\n" + "PRIMARY KEY(NOMBRE)\r\n" + ")");
			System.out.println("CREATE TABLE PORCENTAJES");

			statement.executeUpdate(
					"ALTER TABLE POBLACION ADD CONSTRAINT\r\n" + "FOREIGN KEY(NOMBRE) REFERENCES PAIS(NOMBRE)\r\n"
							+ "ON UPDATE CASCADE ON DELETE CASCADE\r\n" + "");
			System.out.println("ALTER TABLE POBLACION");

			statement.executeUpdate("ALTER TABLE POBLACIONHOMBRES ADD CONSTRAINT \r\n"
					+ "FOREIGN KEY(NOMBRE) REFERENCES PAIS(NOMBRE)\r\n" + "ON UPDATE CASCADE ON DELETE CASCADE");
			System.out.println("ALTER TABLE POBLACION HOMBRES");

			statement.executeUpdate("ALTER TABLE POBLACIONMUJERES ADD CONSTRAINT\r\n"
					+ "FOREIGN KEY(NOMBRE) REFERENCES PAIS(NOMBRE)\r\n" + "ON UPDATE CASCADE ON DELETE CASCADE");
			System.out.println("ALTER TABLE POBLACION MUJERES");

			statement.executeUpdate("ALTER TABLE PORCENTAJES ADD CONSTRAINT\r\n"
					+ "FOREIGN KEY(NOMBRE) REFERENCES PAIS(NOMBRE)\r\n" + "ON UPDATE CASCADE ON DELETE CASCADE");
			System.out.println("ALTER TABLE PORCENTAJES");

			System.out.println("TABLES CREATED");

			// Insert data
			BufferedReader bufferedReader = new BufferedReader(new FileReader("src\\_1\\Paises.txt"));
			Scanner scanner = new Scanner(bufferedReader);

			while (scanner.hasNextLine()) {
				Scanner lineScanner = new Scanner(scanner.nextLine());
				lineScanner.useDelimiter("\\s*;\\s*");

				String continente = lineScanner.next();
				String pais = lineScanner.next();
				int populacion = Integer.parseInt(lineScanner.next());

				PreparedStatement insertStatement = connectionDatabase
						.prepareStatement("INSERT INTO pais VALUES(?,?,?)");

				insertStatement.setString(1, continente);
				insertStatement.setString(2, pais);
				insertStatement.setInt(3, populacion);

				insertStatement.executeUpdate();

				lineScanner.close();
			}

			System.out.println("INSERT DATA PAIS");
			scanner.close();

			// Poblacion
			bufferedReader = new BufferedReader(new FileReader("src\\_1\\Poblacion.txt"));
			scanner = new Scanner(bufferedReader);

			// Alemania ; 81265 ; 39941 ; 41324
			while (scanner.hasNextLine()) {
				Scanner lineScanner = new Scanner(scanner.nextLine());
				lineScanner.useDelimiter("\\s*;\\s*");

				String pais = lineScanner.next();
				int poblacion = Integer.parseInt(lineScanner.next());
				int poblacionHombres = Integer.parseInt(lineScanner.next());
				int poblacionMujeres = Integer.parseInt(lineScanner.next());

				PreparedStatement insertStatement = connectionDatabase
						.prepareStatement("INSERT INTO poblacion VALUES(?, ?, ?, ?)");

				insertStatement.setString(1, pais);
				insertStatement.setInt(2, poblacion);
				insertStatement.setInt(3, poblacionHombres);
				insertStatement.setInt(4, poblacionMujeres);

				insertStatement.executeUpdate();

				lineScanner.close();
			}

			System.out.println("INSERT DATA POBLACION");

			scanner.close();

			ArrayList<ColumnTypes> columnTypes = new ArrayList<ColumnTypes>(
					Arrays.asList(ColumnTypes.STRING, ColumnTypes.INT, ColumnTypes.INT, ColumnTypes.INT,
							ColumnTypes.INT, ColumnTypes.INT, ColumnTypes.INT));

			InserterConfig inserterConfig = new InserterConfig(connectionDatabase,
					"src\\_1\\PoblacionEdadesMujeres.txt", "poblacionmujeres", columnTypes);

			Inserter inserterEdadesMujeres = new Inserter(inserterConfig);
			inserterEdadesMujeres.insert();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
