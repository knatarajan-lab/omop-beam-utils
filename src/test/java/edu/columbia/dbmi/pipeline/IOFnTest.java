package edu.columbia.dbmi.pipeline;

import edu.columbia.dbmi.utils.NLPSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class IOFnTest extends PTransform<PCollection<Row>, PCollection<Row>> {

  static Schema output_schema = NLPSchema.getNoteNLPSchema();

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    return input
        .apply(ParDo.of(new IOSingleFn()))
        .setRowSchema(output_schema)
        .setCoder(SchemaCoder.of(output_schema));
  }

  public static class IOSingleFn extends DoFn<Row, Row> {
    @ProcessElement
    public void processElement(@Element Row input, OutputReceiver<Row> receiver) {
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      DateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Date date = new Date(2022 - 1900, Calendar.NOVEMBER, 10, 10, 10, 10);
      Row output =
          Row.withSchema(output_schema)
              .addValue(0L)
              .addValue(input.getValue("note_id"))
              .addValue(0L)
              .addValue("test_snippet")
              .addValue("0-100")
              .addValue("test_variant")
              .addValue(100L)
              .addValue(100L)
              .addValue("NLP 1.0.0")
              .addValue(dateFormat.format(date))
              .addValue(datetimeFormat.format(date))
              .addValue("False")
              .addValue("1 year")
              .addValue("TermModifier")
              .build();
      receiver.output(output);
    }
  }
}
