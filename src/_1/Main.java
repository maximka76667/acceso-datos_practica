package _1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Main {

	public static void main(String[] args) {

		final String url = "jdbc:mysql://localhost";
		final String db = "POBLACION";

		Properties props = new Properties();
		props.put("user", "root");
		props.put("password", "root");
		props.put("useSSL", "false");

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
			// Insert paises

			// Ejemplo:
			// Africa ; Argelia ; 2381740
			// String, String, Int
			ArrayList<ColumnTypes> paisesColumnTypes = new ArrayList<ColumnTypes>(
					Arrays.asList(ColumnTypes.STRING, ColumnTypes.STRING, ColumnTypes.INT));

			InserterConfig paisesInserterConfig = new InserterConfig(connectionDatabase, "src\\_1\\Paises.txt", "pais",
					paisesColumnTypes);

			Inserter inserter = new Inserter(paisesInserterConfig);
			inserter.insert();

			// Insert poblacion

			// Ejemplo:
			// Alemania ; 81265 ; 39941 ; 41324
			ArrayList<ColumnTypes> poblacionColumnTypes = new ArrayList<ColumnTypes>(
					Arrays.asList(ColumnTypes.STRING, ColumnTypes.INT, ColumnTypes.INT, ColumnTypes.INT));

			InserterConfig poblacionInserterConfig = new InserterConfig(connectionDatabase, "src\\_1\\Poblacion.txt",
					"poblacion", poblacionColumnTypes);

			inserter.setConfig(poblacionInserterConfig);
			inserter.insert();

			// Insert poblacion edades hombres

			// Ejemplo:
			// Alemania ; 1627 ; 1708 ; 3091 ; 5613 ; 17815 ; 10087
			ArrayList<ColumnTypes> poblacionEdadesHombresColumnTypes = new ArrayList<ColumnTypes>(
					Arrays.asList(ColumnTypes.STRING, ColumnTypes.INT, ColumnTypes.INT, ColumnTypes.INT,
							ColumnTypes.INT, ColumnTypes.INT, ColumnTypes.INT));

			InserterConfig poblacionEdadesHombresInserterConfig = new InserterConfig(connectionDatabase,
					"src\\_1\\PoblacionEdadesHombres.txt", "poblacionhombres", poblacionEdadesHombresColumnTypes);

			inserter.setConfig(poblacionEdadesHombresInserterConfig);
			inserter.insert();

			// Insert poblacion edades mujeres

			// Ejemplo:
			// Alemania ; 1546 ; 1627 ; 2928 ; 5369 ; 17245 ; 12609
			ArrayList<ColumnTypes> poblacionEdadesMujeresColumnTypes = new ArrayList<ColumnTypes>(
					Arrays.asList(ColumnTypes.STRING, ColumnTypes.INT, ColumnTypes.INT, ColumnTypes.INT,
							ColumnTypes.INT, ColumnTypes.INT, ColumnTypes.INT));

			InserterConfig poblacionEdadesMujeresInserterConfig = new InserterConfig(connectionDatabase,
					"src\\_1\\PoblacionEdadesMujeres.txt", "poblacionmujeres", poblacionEdadesMujeresColumnTypes);

			inserter.setConfig(poblacionEdadesMujeresInserterConfig);
			inserter.insert();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
