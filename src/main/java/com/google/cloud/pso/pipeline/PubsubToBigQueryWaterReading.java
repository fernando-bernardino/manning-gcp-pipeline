/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pso.pipeline;

import com.google.api.services.bigquery.model.TableRow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * The {@link PubsubToBigQueryWaterReading} is a streaming pipeline which dynamically routes
 * messages to their output location using an attribute within the Pub/Sub message header. This
 * pipeline requires any tables which will be routed to, to be defined prior to execution.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Pub/Sub subscription must exist prior to pipeline execution.
 *   <li>The BigQuery output tables routed to must be created prior to pipeline execution.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT_ID
 * PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pipelines/pubsub-to-bigquery-dynamic-destinations
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.pso.pipeline.PubsubToBigQueryDynamicDestinations \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --runner=${RUNNER} \
 * --subscription=SUBSCRIPTION \
 * --waterHeight=WATER_HEIGHT \
 * --tableName=TABLE_NAME \
 * --tableName=TABLE_NAME \
 * --outputTableProject=PROJECT \
 * --outputTableDataset=DATASET"
 * </pre>
 */
public class PubsubToBigQueryWaterReading {

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions {

    @Description("The Pub/Sub subscription to read messages from")
    @Required
    String getSubscription();

    void setSubscription(String value);

    @Description(
      "Water height required to trigger an alert.")
    @Required
    Double getWaterHeight();

    void setWaterHeight(Double value);

    @Description(
      "The name of the attribute which will contain the table name to route the message to.")
    @Required
    String getTableName();

    void setTableName(String value);

    @Description(
        "The name of the attribute which will contain the table name to route the message to.")
    @Required
    String getOutputTableProject();

    void setOutputTableProject(String value);

    @Description(
        "The name of the attribute which will contain the table name to route the message to.")
    @Required
    String getOutputTableDataset();

    void setOutputTableDataset(String value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * PubsubToBigQueryWaterReading#run(Options)} method to start the pipeline and invoke
   * {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Retrieve non-serializable parameters
    String table = String.format("%s:%s.%s",
      options.getOutputTableProject(),
      options.getOutputTableDataset(),
      options.getTableName());

    // Build & execute pipeline
    pipeline
        .apply("ReadMessages", PubsubIO.readMessagesWithAttributes()
            .fromSubscription(options.getSubscription()))
        .apply("MapToRecord", ParDo.of(new PubsubMessageToTableRow()))
        .apply("AddAlertTrigger", ParDo.of(new AddAlertColumn(options.getWaterHeight())))
        .apply("WriteToBigQuery",BigQueryIO.<TableRow>write().to(table)
            .withFormatFunction((row) -> row)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    return pipeline.run();
  }

  static class PubsubMessageToTableRow extends DoFn<PubsubMessage, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      TableRow row;
      String payload = new String(message.getPayload());
      // Parse the JSON into a {@link TableRow} object.
      try (InputStream inputStream = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8))) {
        row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
      } catch (IOException e) {
        throw new RuntimeException("Failed to serialize json to table row: " + payload, e);
      }
      context.output(row);
    }
  }

  static class AddAlertColumn extends DoFn<TableRow, TableRow> {
    private final Double waterHeight;
    public AddAlertColumn(Double waterHeight) {
      this.waterHeight = waterHeight;
    }
    @ProcessElement
    public void processElement(ProcessContext context) {
      TableRow row = context.element();
      double height = (double) row.get("maximum_water_height");
      row.set("trigger_alert", height >= waterHeight);
      context.output(row);
    }
  }
}
