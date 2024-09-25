package edu.columbia.dbmi.io.factory;

import edu.columbia.dbmi.io.bigquery.BigQueryWrite;
import edu.columbia.dbmi.io.csv.CSVWrite;
import edu.columbia.dbmi.io.jsonl.JSONLWrite;
import edu.columbia.dbmi.io.parquet.ParquetWrite;

public class IOWriteFactory {
  public static IOWrite create(String output_type) {
    if ("csv".equalsIgnoreCase(output_type)) {
      return new CSVWrite();
    } else if ("jsonl".equalsIgnoreCase(output_type)) {
      return new JSONLWrite();
    } else if ("parquet".equalsIgnoreCase(output_type)) {
      return new ParquetWrite();
    } else if ("bigquery".equalsIgnoreCase(output_type)) {
      return new BigQueryWrite();
    }
    return null;
  }
}
