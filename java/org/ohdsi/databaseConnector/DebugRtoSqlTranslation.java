package org.ohdsi.databaseConnector;

public class DebugRtoSqlTranslation {

	public static void main(String[] args) {
//		String sql = "SELECT dateAdd('day' AS unit, 1, start_date AS date) AS start_date FROM my_table;";
//		String sql = "SELECT dateAdd(1 AS value, start_date AS date, 'day' AS unit) AS start_date FROM my_table;";
//		String sql = "SELECT DatabaseConnector::dateAdd(1 AS value, DatabaseConnector::dateAdd('day', 1, a_date) AS date, 'day' AS unit) AS start_date FROM my_table;";
//		String sql = "SELECT dateDiff('day', a_date, another_date) AS start_date FROM my_table;";
		String sql = "SELECT TOP 11 *\nFROM cdmv5.observation_period\nWHERE (DATEDIFF(day, observation_period_start_date, observation_period_end_date) > 365.0);";
//		RFunctionToTranslate rFunctionToTranslate = new RFunctionToTranslate("dateDiff");
//		rFunctionToTranslate.addArgument("interval", true);
//		rFunctionToTranslate.addArgument("value");
//		rFunctionToTranslate.addArgument("date");
		RFunctionToTranslate rFunctionToTranslate = new RFunctionToTranslate("day");
		rFunctionToTranslate.addArgument("date");
		RtoSqlTranslator rtoSqlTranslator = new RtoSqlTranslator(sql);
		rtoSqlTranslator.translate(rFunctionToTranslate);
		System.out.println(rtoSqlTranslator.getSql());

	}

}
