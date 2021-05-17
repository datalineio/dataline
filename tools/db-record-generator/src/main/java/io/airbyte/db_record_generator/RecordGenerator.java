package io.airbyte.db_record_generator;

import io.airbyte.db.Databases;
import io.airbyte.integrations.destination.jdbc.DefaultSqlOperations;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public class RecordGenerator {
  private static String jdbcUrl = "jdbc:postgresql://davin-database-1.cuczxc1ksccg.us-east-2.rds.amazonaws.com:5432/postgres";
  private static String user = "postgres";
  private static String password = "airbyte-data-1";

  private static int numRecords = 1_000_000;
  private static int insertBatch = 1000;
  private static int columnLimit = 30_000;
  private static String fullyQualifiedTableName = "public.large_table";

  public static void insertLargeTableRecords() throws Exception {
    var sqlOps = new DefaultSqlOperations();
    var postgresDB = Databases.createJdbcDatabase(user, password, jdbcUrl, "org.postgresql.Driver");

    for (int totalInsertedRecs = 0; totalInsertedRecs < numRecords; totalInsertedRecs+=insertBatch) {
      var queries = new ArrayList<String>();
      for (int currNumInsertQuery = 0; currNumInsertQuery < insertBatch; currNumInsertQuery++) {
        var id = UUID.randomUUID().toString();
        var title = StringUtils.repeat('*', columnLimit);
        var did = StringUtils.repeat('*', columnLimit);
        var date_prod = StringUtils.repeat('*', columnLimit);
        var kind = StringUtils.repeat('*', columnLimit);
        var len = StringUtils.repeat('*', columnLimit);

        queries.add(String.format("INSERT INTO %s (code, title, did, date_prod, kind, len) VALUES" +
            "('%s', '%s', '%s', '%s', '%s', '%s');\n", fullyQualifiedTableName, id, title, did, date_prod, kind, len));
      }
      sqlOps.executeTransaction(postgresDB, queries);
      if (totalInsertedRecs % 10000 == 0)
        System.out.println("inserted: " + totalInsertedRecs);
    }
  }

  public static void insertLargeRowRecords() throws Exception {
    var colNum = 80;
    var sqlOps = new DefaultSqlOperations();
    var postgresDB = Databases.createJdbcDatabase(user, password, jdbcUrl, "org.postgresql.Driver");

    for (int totalInsertedRecs = 0; totalInsertedRecs < numRecords; totalInsertedRecs+=insertBatch) {
      var queries = new ArrayList<String>();
      for (int currNumInsertQuery = 0; currNumInsertQuery < insertBatch; currNumInsertQuery++) {
        var id = UUID.randomUUID().toString();
        var prefix = "INSERT INTO public.large_row VALUES";
        var suffix = new StringBuffer(prefix);
        suffix.append("(");
        suffix.append("'" + id + "',");
        for (int i = 0; i < colNum; i++) {
          suffix.append("'" + StringUtils.repeat('*', columnLimit) + "'");
          if (i < colNum-1) {
            suffix.append(", ");
          }
        }
        suffix.append(");\n");
        queries.add(suffix.toString());
      }
      sqlOps.executeTransaction(postgresDB, queries);
      if (totalInsertedRecs % 10000 == 0)
        System.out.println("inserted: " + totalInsertedRecs);
    }
  }

  public static void main(String[] args) throws Exception {
    insertLargeRowRecords();
  }

}
