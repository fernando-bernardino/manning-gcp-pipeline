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
import com.google.cloud.pso.pipeline.PubsubToBigQueryWaterReading.*;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test cases for the {@link PubsubToBigQueryWaterReading}
 * class.
 */
@RunWith(JUnit4.class)
public class PubsubToBigQueryWaterReadingTest {
  public static final double MAXIMUM_WATER_HEIGHT = 10.0;

  @Test
  public void testToTableRowTransform() {
    final String payload = "{\"earthquake_magnitude\":5.745714,\"latitude\":41.58692,\"maximum_water_height\":0.028957665,\"tsunami_cause_code\":11,\"tsunami_event_validity\":4,\"timestamp\":\"2021-05-21 19:14:20\",\"longitude\":87.570564}";
    final PubsubMessage message =
      new PubsubMessage(payload.getBytes(), ImmutableMap.of("id", "123", "type", "reading_event"));
    ProcessContext context = mock(ProcessContext.class);
    when(context.element()).thenReturn(message);

    new PubsubMessageToTableRow().processElement(context);
  }

  @Test
  public void testAddTriggerColumnFalseBelowTenMeters() {
    final String payload = "{\"earthquake_magnitude\":5.745714,\"latitude\":41.58692,\"maximum_water_height\":9.99999,\"tsunami_cause_code\":11,\"tsunami_event_validity\":4,\"timestamp\":\"2021-05-21 19:14:20\",\"longitude\":87.570564}";
    TableRow row = convertJsonToTableRow(payload);

    ProcessContext context = mock(ProcessContext.class);
    when(context.element()).thenReturn(row);

    new AddAlertColumn(MAXIMUM_WATER_HEIGHT).processElement(context);

    assertFalse((boolean) row.get("trigger_alert"));
  }

  @Test
  public void testAddTriggerColumnTrueAboveTenMeters() {
    final String payload = "{\"earthquake_magnitude\":5.745714,\"latitude\":41.58692,\"maximum_water_height\":10.0,\"tsunami_cause_code\":11,\"tsunami_event_validity\":4,\"timestamp\":\"2021-05-21 19:14:20\",\"longitude\":87.570564}";
    TableRow row = convertJsonToTableRow(payload);

    ProcessContext context = mock(ProcessContext.class);
    when(context.element()).thenReturn(row);

    new AddAlertColumn(MAXIMUM_WATER_HEIGHT).processElement(context);

    assertTrue((boolean) row.get("trigger_alert"));
  }

  private TableRow convertJsonToTableRow(String json) {
    // Parse the JSON into a {@link TableRow} object.
    try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
      return TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize json to table row: " + json, e);
    }
  }
}
