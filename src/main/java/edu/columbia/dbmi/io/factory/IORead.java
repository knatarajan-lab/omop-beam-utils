package edu.columbia.dbmi.io.factory;

import edu.columbia.dbmi.utils.NLPSchema;
import edu.columbia.dbmi.utils.SanitizeInput;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public abstract class IORead extends PTransform<PBegin, PCollection<Row>> {

  public static Schema input_schema = NLPSchema.getNoteSchema();
  public String input_pattern;

  public void init(String input_dir, String input_type) {
    input_dir = SanitizeInput.sanitize(input_dir);
    if ("bigquery".equalsIgnoreCase(input_type)) {
      input_pattern = input_dir;
    } else {
      input_pattern = input_dir + "/*" + input_type;
    }
  }

  public abstract PCollection<Row> expand(PBegin input);
}
