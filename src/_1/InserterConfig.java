package _1;

import java.sql.Connection;
import java.util.ArrayList;

public class InserterConfig {

	private Connection connection;
	private String path;
	private String db;
	private ArrayList<ColumnTypes> columnsTypes;

	public InserterConfig(Connection connection, String path, String db, ArrayList<ColumnTypes> columnsTypes) {
		this.connection = connection;
		this.path = path;
		this.db = db;
		this.columnsTypes = columnsTypes;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public ArrayList<ColumnTypes> getColumnsTypes() {
		return columnsTypes;
	}

	public void setColumnsTypes(ArrayList<ColumnTypes> columnsTypes) {
		this.columnsTypes = columnsTypes;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

}
