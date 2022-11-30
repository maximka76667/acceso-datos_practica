package _1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

interface IInserterController {
	void onInsert(Inserter inserter, Scanner lineScanner) throws SQLException;
}

public class Inserter {
	private BufferedReader reader;
	private Scanner mainScanner;
	private InserterConfig config;

	public Inserter(InserterConfig config) {
		setPath(config.getPath());
		this.config = config;
	}

	public void insert() throws SQLException {
		while (mainScanner.hasNextLine()) {
			Scanner lineScanner = new Scanner(mainScanner.nextLine());
			lineScanner.useDelimiter("\\s*;\\s*");

			String sqlQuery = "INSERT INTO " + config.getDb() + " VALUES(";
			for (int i = 0; i < config.getColumnsTypes().size(); i++) {
				sqlQuery += "?";
				if (i < config.getColumnsTypes().size() - 1) {
					sqlQuery += ", ";
				}
			}
			sqlQuery += ")";

			PreparedStatement insertStatement = config.getConnection().prepareStatement(sqlQuery);

			for (int i = 1; i <= config.getColumnsTypes().size(); i++) {
				ColumnTypes columnType = config.getColumnsTypes().get(i - 1);
				if (columnType == ColumnTypes.STRING) {
					insertStatement.setString(i, lineScanner.next());
				} else if (columnType == ColumnTypes.INT) {
					insertStatement.setInt(i, nextInt(lineScanner));
				}
			}

			insertStatement.executeUpdate();

			lineScanner.close();
		}
	}

	public int nextInt(Scanner scanner) {
		return Integer.parseInt(scanner.next());
	}

	public void setPath(String path) {
		try {
			this.reader = new BufferedReader(new FileReader(path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		this.mainScanner = new Scanner(reader);
	}

}
