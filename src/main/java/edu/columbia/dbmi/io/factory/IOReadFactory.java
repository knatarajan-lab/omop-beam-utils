package edu.columbia.dbmi.io.factory;

import edu.columbia.dbmi.io.bigquery.BigQueryRead;
import edu.columbia.dbmi.io.csv.CSVRead;
import edu.columbia.dbmi.io.jsonl.JSONLRead;
import edu.columbia.dbmi.io.parquet.ParquetRead;

public class IOReadFactory {
  public static IORead create(String input_type) {
    if ("csv".equalsIgnoreCase(input_type)) {
      return new CSVRead();
    } else if ("jsonl".equalsIgnoreCase(input_type)) {
      return new JSONLRead();
    } else if ("parquet".equalsIgnoreCase(input_type)) {
      return new ParquetRead();
    } else if ("bigquery".equalsIgnoreCase(input_type)) {
      return new BigQueryRead();
    }
    return null;
  }
}
