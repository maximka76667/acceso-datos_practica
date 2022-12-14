package _2;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class Main {

	public static void main(String[] args) {

		try {
			Properties props = new Properties();
			props.put("user", "root");
			props.put("password", "root");

			Connection cnt = DriverManager.getConnection("jdbc:mysql://localhost/poblacion", props);

			// 1
			CallableStatement cs = cnt.prepareCall(
					"SELECT PAIS.CONTINENTE,PAIS.NOMBRE,POBLACION,\r\n" + "POBLACIONHOMBRES,POBLACIONMUJERES,\r\n"
							+ "PORCENTAJEHOMBRES,PORCENTAJEMUJERES\r\n" + "FROM PAIS,POBLACION,PORCENTAJES\r\n"
							+ "WHERE PAIS.NOMBRE=POBLACION.NOMBRE AND PAIS.NOMBRE=PORCENTAJES.NOMBRE\r\n"
							+ "ORDER BY PAIS.CONTINENTE,PAIS.NOMBRE");

			PrintWriter writer = new PrintWriter(new FileWriter("src\\_2\\PorcentajesPoblacionPaises.txt"), false);

			ResultSet result = cs.executeQuery();
			writer.format("%-15s%-40s%10s%10s%10s%10s%% %10s%% %n", "Continente", "Pais", "Total", "Hombres", "Mujeres",
					"%Hombres", "%Mujeres");

			while (result.next()) {
				writer.format("%-15s%-40s%10s%10s%10s%10.2f%% %10.2f%% %n", result.getString(1), result.getString(2),
						result.getString(3), result.getString(4), result.getString(5), result.getDouble(6),
						result.getDouble(7));
			}
			System.out.println("PorcentajesPoblacionPaises DONE");
			writer.close();

			// 2
			cs = cnt.prepareCall(
					"SELECT CONTINENTE,\r\n" + "SUM(POBLACION) ,SUM(POBLACIONHOMBRES) ,SUM(POBLACIONMUJERES) ,\r\n"
							+ "(SUM(POBLACIONHOMBRES)/SUM(POBLACION))*100,\r\n"
							+ "(SUM(POBLACIONMUJERES)/SUM(POBLACION))*100\r\n" + "FROM POBLACION,PAIS\r\n"
							+ "WHERE PAIS.NOMBRE=POBLACION.NOMBRE\r\n" + "GROUP BY CONTINENTE");

			writer = new PrintWriter(new FileWriter("src\\_2\\PorcentajesPoblacionContinentes.txt"), false);

			result = cs.executeQuery();

			writer.format("%-20s %15s %10s %10s %11s %11s %n", "Continente", "Total", "Hombres", "Mujeres", "%Hombres",
					"%Mujeres");

			while (result.next()) {
				writer.format("%-20s %15s %10s %10s %10.2f%% %10.2f%% %n", "Total " + result.getString(1),
						result.getString(2), result.getString(3), result.getString(4), result.getDouble(5),
						result.getDouble(6));
			}

			cs = cnt.prepareCall("SELECT SUM(POBLACION) ,SUM(POBLACIONHOMBRES) ,SUM(POBLACIONMUJERES) ,\r\n"
					+ "(SUM(POBLACIONHOMBRES)/SUM(POBLACION))*100,\r\n"
					+ "(SUM(POBLACIONMUJERES)/SUM(POBLACION))*100\r\n" + "FROM POBLACION\r\n" + "");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-20s %15s %10s %10s %10.2f%% %10.2f%% %n", "Total general ", result.getString(1),
						result.getString(2), result.getString(3), result.getDouble(4), result.getDouble(5));
			}

			writer.close();
			System.out.println("PorcentajesPoblacionContinentes DONE");

			// 3
			cs = cnt.prepareCall("SELECT PAIS.CONTINENTE,PAIS.NOMBRE,\r\n"
					+ "DENSIDADTOTAL,DENSIDADHOMBRES,DENSIDADMUJERES\r\n" + "FROM PAIS,PORCENTAJES\r\n"
					+ "WHERE PAIS.NOMBRE=PORCENTAJES.NOMBRE\r\n" + "ORDER BY PAIS.CONTINENTE,PAIS.NOMBRE");

			writer = new PrintWriter(new FileWriter("src\\_2\\PorcentajesDensidadPaises.txt"), false);

			result = cs.executeQuery();
			writer.format("%-15s %-40s %18s %18s %18s %n", "Continente", "Pais", "Densidad Total", "Densidad Hombres",
					"Densidad Mujeres");

			while (result.next()) {
				writer.format("%-15s %-40s %10.2f por km2 %10.2f por km2 %10.2f por km2 %n", result.getString(1),
						result.getString(2), result.getDouble(3), result.getDouble(4), result.getDouble(5));
			}
			System.out.println("PorcentajesPoblacionPaises DONE");
			writer.close();

			// 4
			cs = cnt.prepareCall("SELECT CONTINENTE,SUM(SUPERFICIE),\r\n" + "(SUM(POBLACION)/SUM(SUPERFICIE))*1000,\r\n"
					+ "(SUM(POBLACIONHOMBRES)/SUM(SUPERFICIE))*1000,\r\n"
					+ "(SUM(POBLACIONMUJERES)/SUM(SUPERFICIE))*1000\r\n" + "FROM POBLACION,PAIS\r\n"
					+ "WHERE PAIS.NOMBRE=POBLACION.NOMBRE\r\n" + "GROUP BY CONTINENTE");

			writer = new PrintWriter(new FileWriter("src\\_2\\PorcentajesDensidadContinentes.txt"), false);

			result = cs.executeQuery();
			writer.format("%-15s %40s %18s %18s %18s %n", "Continente", "Superficie", "Densidad Total",
					"Densidad Hombres", "Densidad Mujeres");

			while (result.next()) {
				writer.format("%-15s %40d %10.2f por km2 %10.2f por km2 %10.2f por km2 %n",
						"Total " + result.getString(1), result.getInt(2), result.getDouble(3), result.getDouble(4),
						result.getDouble(5));
			}

			// General
			cs = cnt.prepareCall("SELECT SUM(SUPERFICIE),\r\n" + "(SUM(POBLACION)/SUM(SUPERFICIE))*1000,\r\n"
					+ "(SUM(POBLACIONHOMBRES)/SUM(SUPERFICIE))*1000,\r\n"
					+ "(SUM(POBLACIONMUJERES)/SUM(SUPERFICIE))*1000\r\n" + "FROM POBLACION,PAIS\r\n"
					+ "WHERE PAIS.NOMBRE=POBLACION.NOMBRE\r\n" + "");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-15s %40d %10.2f por km2 %10.2f por km2 %10.2f por km2 %n", "Total general",
						result.getInt(1), result.getDouble(2), result.getDouble(3), result.getDouble(4));
			}

			System.out.println("PorcentajesDensidadContinentes DONE");
			writer.close();

			// 5
			writer = new PrintWriter(new FileWriter("src\\_2\\EstadisticasPaises.txt"), false);
			writer.format("%-45s %-30s %-30s %n", "Pregunta", "Pais", "Dato");

			// Mas extenso
			cs = cnt.prepareCall(
					"SELECT NOMBRE,SUPERFICIE FROM PAIS\r\n" + "WHERE SUPERFICIE=(SELECT MAX(SUPERFICIE) FROM PAIS)");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %16d km2 %n", "Mas extenso", result.getString(1), result.getInt(2));
			}

			// Mas pequeno
			cs = cnt.prepareCall(
					"SELECT NOMBRE,SUPERFICIE FROM PAIS\r\n" + "WHERE SUPERFICIE=(SELECT MIN(SUPERFICIE) FROM PAIS)");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %16d km2 %n", "Mas pequeno", result.getString(1), result.getInt(2));
			}

			// Mas poblado
			cs = cnt.prepareCall("SELECT NOMBRE,POBLACION FROM POBLACION\r\n"
					+ "WHERE POBLACION=(SELECT MAX(POBLACION) FROM POBLACION)");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %20d %n", "Más Poblado", result.getString(1), result.getInt(2));
			}

			// Menos poblado
			cs = cnt.prepareCall("SELECT NOMBRE,POBLACION FROM POBLACION\r\n"
					+ "WHERE POBLACION=(SELECT MIN(POBLACION) FROM POBLACION)");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %20d %n", "Menos poblado", result.getString(1), result.getInt(2));
			}

			// Mayor Densidad de Población:
			cs = cnt.prepareCall("SELECT NOMBRE,DENSIDADTOTAL FROM PORCENTAJES\r\n"
					+ "WHERE DENSIDADTOTAL=(SELECT MAX(DENSIDADTOTAL) FROM PORCENTAJES)");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %12.2f por km2 %n", "Mayor Densidad de Población", result.getString(1),
						result.getDouble(2));
			}

			// Menor Densidad de Población:
			cs = cnt.prepareCall("SELECT NOMBRE,DENSIDADTOTAL FROM PORCENTAJES\r\n"
					+ "WHERE DENSIDADTOTAL=(SELECT MIN(DENSIDADTOTAL) FROM PORCENTAJES)\r\n" + "");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %12.2f por km2 %n", "Menor Densidad de Población", result.getString(1),
						result.getDouble(2));
			}

			// Mayor Densidad de Hombres:

			cs = cnt.prepareCall("SELECT NOMBRE,DENSIDADHOMBRES FROM PORCENTAJES\r\n"
					+ "WHERE DENSIDADHOMBRES=(SELECT MAX(DENSIDADHOMBRES) FROM PORCENTAJES)\r\n");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %12.2f por km2 %n", "Mayor Densidad de Hombres", result.getString(1),
						result.getDouble(2));
			}

			// Mayor Densidad de Mujeres
			cs = cnt.prepareCall("SELECT NOMBRE,DENSIDADMUJERES FROM PORCENTAJES\r\n"
					+ "WHERE DENSIDADMUJERES=(SELECT MAX(DENSIDADMUJERES) FROM PORCENTAJES)\r\n" + "");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %12.2f por km2 %n", "Mayor Densidad de Mujeres", result.getString(1),
						result.getDouble(2));
			}

			// Mayor Porcentaje de Hombres
			cs = cnt.prepareCall("SELECT NOMBRE,PORCENTAJEHOMBRES FROM PORCENTAJES\r\n"
					+ "WHERE PORCENTAJEHOMBRES=(SELECT MAX(PORCENTAJEHOMBRES) FROM PORCENTAJES)\r\n" + "");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %19.2f%% %n", "Mayor Porcentaje de Hombres", result.getString(1),
						result.getDouble(2));
			}

			// Mayor Porcentaje de Mujeres:
			cs = cnt.prepareCall("SELECT NOMBRE,PORCENTAJEMUJERES FROM PORCENTAJES\r\n"
					+ "WHERE PORCENTAJEMUJERES=(SELECT MAX(PORCENTAJEMUJERES) FROM PORCENTAJES)");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %19.2f%% %n", "Mayor Porcentaje de Mujeres", result.getString(1),
						result.getDouble(2));
			}

			// Mas viejo
			cs = cnt.prepareCall("SELECT NOMBRE,PORCENTAJEVIEJOS FROM PORCENTAJES\r\n"
					+ "WHERE PORCENTAJEVIEJOS=(SELECT MAX(PORCENTAJEVIEJOS) FROM PORCENTAJES)");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %19.2f%% %n", "Mas viejo", result.getString(1), result.getDouble(2));
			}

			// Más Joven
			cs = cnt.prepareCall("SELECT NOMBRE,PORCENTAJEJOVENES FROM PORCENTAJES\r\n"
					+ "WHERE PORCENTAJEJOVENES=(SELECT MAX(PORCENTAJEJOVENES) FROM PORCENTAJES)");

			result = cs.executeQuery();

			while (result.next()) {
				writer.format("%-45s %-30s %19.2f%% %n", "Más Joven", result.getString(1), result.getDouble(2));
			}

			// Mayor Porcentaje de Hombres entre 18 y 29
			cs = cnt.prepareCall("SELECT NOMBRE,PORCENTAJE_18_29HOMBRES FROM PORCENTAJES\r\n"
					+ "WHERE PORCENTAJE_18_29HOMBRES=(SELECT MAX(PORCENTAJE_18_29HOMBRES) FROM \r\n" + "PORCENTAJES)");

			result = cs.executeQuery();

			if (result.next()) {
				writer.format("%-45s %-30s %19.2f%% %n", "Mayor Porcentaje de Hombres entre 18 y 29",
						result.getString(1), result.getDouble(2));
			}

			// Mayor Porcentaje de Mujeres entre 18 y 29
			cs = cnt.prepareCall("SELECT NOMBRE,PORCENTAJE_18_29MUJERES FROM PORCENTAJES\r\n"
					+ "WHERE PORCENTAJE_18_29MUJERES=(SELECT MAX(PORCENTAJE_18_29MUJERES) FROM \r\n" + "PORCENTAJES)");

			result = cs.executeQuery();

			if (result.next()) {
				writer.format("%-45s %-30s %19.2f%% %n", "Mayor Porcentaje de Mujeres entre 18 y 29",
						result.getString(1), result.getDouble(2));
			}

			System.out.println("EstadisticasPaises DONE");
			writer.close();

		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

}
