package edu.columbia.dbmi.pipeline;

import com.google.cloud.bigquery.*;
import junit.framework.TestCase;
import edu.columbia.dbmi.io.factory.IORead;
import edu.columbia.dbmi.io.factory.IOReadFactory;
import edu.columbia.dbmi.io.factory.IOWrite;
import edu.columbia.dbmi.io.factory.IOWriteFactory;
import edu.columbia.dbmi.utils.NLPSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static edu.columbia.dbmi.utils.Constants.Env.*;

@Ignore
public class BigQueryIOTest extends TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOTest.class);

  BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
  String input_table = PROJECT_ID + "." + TEST_DATASET + "." + TEST_INPUT_TABLE;
  String output_table = PROJECT_ID + "." + TEST_DATASET + "." + TEST_OUTPUT_TABLE;

  @Before
  public void setup() throws InterruptedException {

    // Delete input and output table contents
    QueryJobConfiguration delQueryConfig =
        QueryJobConfiguration.newBuilder("DELETE FROM `" + input_table + "`" + " WHERE TRUE;" +
            "DELETE FROM `" + output_table + "`" + " WHERE TRUE;")
            .setUseLegacySql(false)
            .build();

    JobId delJobId = JobId.of(UUID.randomUUID().toString());
    Job delQueryJob = bigquery.create(JobInfo.newBuilder(delQueryConfig).setJobId(delJobId).build());

    delQueryJob.waitFor();

    // Insert data into input table
    QueryJobConfiguration inQueryConfig =
      QueryJobConfiguration.newBuilder("INSERT INTO `" + input_table + "`" + " " +
          "VALUES (1001, 1, '2020-10-10', '2020-10-10 01:01:01 UTC', 3522232, 0, 'Administrative note', " +
          "'This note contains a few characters like headache.This note contains a few characters like headache. " +
          "This note contains a few characters like headache', " +
          "1147732, 4180186, 1234, 123456, 1234567, 5), " +
          "(3002, 2, '2020-10-10', '2020-10-10 01:01:01 UTC', 3522232, 0, 'Administrative note', " +
          "'This is a different note that contains a few characters like acne.This is a different note that contains " +
          "a few characters like acne. This is a different note that contains a few characters like acne', " +
          "1147732, 4180186, 1234, 23456, 234567, 6)")
        .setUseLegacySql(false)
        .build();

    JobId inJobId = JobId.of(UUID.randomUUID().toString());
    Job inQueryJob = bigquery.create(JobInfo.newBuilder(inQueryConfig).setJobId(inJobId).build());

    inQueryJob.waitFor();
  }

  @Test
  public void testReadWriteBQ() throws InterruptedException {

    Schema note_nlp_schema = NLPSchema.getNoteNLPSchema();

    List<String> expected = new ArrayList<>();
    JSONObject json_row_1 = new JSONObject();
    json_row_1.put("note_nlp_id", "0");
    json_row_1.put("note_id", "1001");
    json_row_1.put("section_concept_id", "0");
    json_row_1.put("snippet", "test_snippet");
    json_row_1.put("offset", "0-100");
    json_row_1.put("lexical_variant", "test_variant");
    json_row_1.put("note_nlp_concept_id", "100");
    json_row_1.put("note_nlp_source_concept_id", "100");
    json_row_1.put("nlp_system", "NLP 1.0.0");
    json_row_1.put("nlp_date", "2022-11-10");
    json_row_1.put("nlp_datetime", "2022-11-10 10:10:10");
    json_row_1.put("term_exists", "False");
    json_row_1.put("term_temporal", "1 year");
    json_row_1.put("term_modifiers", "TermModifier");
    expected.add(json_row_1.toString());

    JSONObject json_row_2 = new JSONObject();
    json_row_2.put("note_nlp_id", "0");
    json_row_2.put("note_id", "3002");
    json_row_2.put("section_concept_id", "0");
    json_row_2.put("snippet", "test_snippet");
    json_row_2.put("offset", "0-100");
    json_row_2.put("lexical_variant", "test_variant");
    json_row_2.put("note_nlp_concept_id", "100");
    json_row_2.put("note_nlp_source_concept_id", "100");
    json_row_2.put("nlp_system", "NLP 1.0.0");
    json_row_2.put("nlp_date", "2022-11-10");
    json_row_2.put("nlp_datetime", "2022-11-10 10:10:10");
    json_row_2.put("term_exists", "False");
    json_row_2.put("term_temporal", "1 year");
    json_row_2.put("term_modifiers", "TermModifier");
    expected.add(json_row_2.toString());

    String gcpTempLocation = "gs://" + TEST_BUCKET + "/bq_tmp";
    String[] args = new String[] {"--project=" + PROJECT_ID, "--tempLocation=" + gcpTempLocation};
    TestPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TestPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    String input_output_type = "bigquery";

    CoderRegistry cr = p.getCoderRegistry();
    cr.registerCoderForClass(Integer.class, VarIntCoder.of());
    cr.registerCoderForClass(Long.class, VarLongCoder.of());
    cr.registerCoderForClass(Float.class, DoubleCoder.of());

    IORead ioRead = IOReadFactory.create(input_output_type);
    ioRead.init(input_table, input_output_type);
    IOWrite ioWrite = IOWriteFactory.create(input_output_type);
    ioWrite.init(output_table, input_output_type, 100, 60);

    p.apply(ioRead).apply(new IOFnTest()).apply(ioWrite);

    p.run().waitUntilFinish();

    // Validate
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder("SELECT * FROM `" + output_table + "`" + " WHERE TRUE")
            .setUseLegacySql(false)
            .build();

    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob = queryJob.waitFor();

    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    TableResult result = queryJob.getQueryResults();

    List<String> actual = new ArrayList<>();
    for (FieldValueList tableRow : result.iterateAll()) {
      JSONObject row = new JSONObject();
      for (Schema.Field field : note_nlp_schema.getFields()) {
        if (Objects.equals(field.getName(), "nlp_datetime")) {
          long epoch = Double.valueOf(tableRow.get(field.getName()).getStringValue()).longValue();
          ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epoch), ZoneOffset.UTC);
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
          row.put(field.getName(), zonedDateTime.format(formatter));
        } else {
          row.put(field.getName(), tableRow.get(field.getName()).getStringValue());
        }
      }
      actual.add(row.toString());
    }

    assertTrue(actual.containsAll(expected) && expected.containsAll(actual));
  }

  @After
  public void tearDown() throws InterruptedException {

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder("DELETE FROM `" + input_table + "`" + " WHERE TRUE;" +
            "DELETE FROM `" + output_table + "`" + " WHERE TRUE;")
            .setUseLegacySql(false)
            .build();

    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob.waitFor();
  }
}
