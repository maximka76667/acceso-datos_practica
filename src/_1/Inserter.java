package _1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;

interface IInserterController {
	void onInsert(Inserter inserter, Scanner lineScanner) throws SQLException;
}

public class Inserter {
	private BufferedReader reader;
	private Scanner mainScanner;
	private InserterConfig config;
	private String dbUri;
	private ArrayList<ColumnTypes> columnsTypes;
	private int paramsAmount;

	private ArrayList<ArrayList<Object>> params;

	public void setPath(String path) {
		try {
			this.reader = new BufferedReader(new FileReader(path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		this.mainScanner = new Scanner(reader);
	}

	public void setConfig(InserterConfig config) {
		this.config = config;
		setPath(config.getPath());
		this.dbUri = config.getDb();
		this.columnsTypes = config.getColumnsTypes();
		this.paramsAmount = columnsTypes.size();
		this.params = new ArrayList<ArrayList<Object>>();
	}

	public Inserter(InserterConfig config) {
		setConfig(config);
	}

	public int nextInt(Scanner scanner) {
		return Integer.parseInt(scanner.next());
	}

	public String createInsertQuery() {
		String insertQuery = "INSERT INTO " + dbUri + " VALUES(";
		for (int i = 0; i < paramsAmount; i++) {
			insertQuery += "?";
			if (i < paramsAmount - 1) {
				insertQuery += ", ";
			}
		}
		insertQuery += ")";
		return insertQuery;
	}

	public void insert() throws SQLException {
		while (mainScanner.hasNextLine()) {
			Scanner lineScanner = new Scanner(mainScanner.nextLine());
			lineScanner.useDelimiter("\\s*;\\s*");

			PreparedStatement insertStatement = config.getConnection().prepareStatement(createInsertQuery());

			ArrayList<Object> params = new ArrayList<Object>();
			for (int i = 1; i <= paramsAmount; i++) {
				ColumnTypes columnType = columnsTypes.get(i - 1);
				Object param = null;
				if (columnType == ColumnTypes.STRING) {
					param = lineScanner.next();
					insertStatement.setString(i, (String) param);
				} else if (columnType == ColumnTypes.INT) {
					param = nextInt(lineScanner);
					insertStatement.setInt(i, (int) param);
				}
				params.add(param);
			}

			this.params.add(params);

			insertStatement.executeUpdate();
			lineScanner.close();
		}

		System.out.println("INSERTED " + dbUri);

	}

	public ArrayList<ArrayList<Object>> getParams() {
		return this.params;
	}
}
