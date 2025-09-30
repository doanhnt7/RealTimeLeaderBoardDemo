// /*
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

// package jobs.operators;

// import jobs.models.Score;
// import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
// import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
// import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
// import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
// import org.apache.flink.api.common.state.ValueState;
// import org.apache.flink.api.common.state.MapState;
// import org.junit.After;
// import org.junit.Before;
// import org.junit.Test;
// import java.util.stream.Collectors;
// import java.lang.reflect.Field;
// import java.util.List;
// import java.util.Map;

// import static org.assertj.core.api.Assertions.assertThat;
// import static org.junit.Assert.assertEquals;

// /**
//  * Comprehensive tests for {@link TotalScoreTimeRangeBoundedPrecedingFunction}.
//  * 
//  * Tests include:
//  * - Correct score calculation (accumulation and retraction)
//  * - State cleanup functionality
//  * - Late data handling
//  * - Timer registration and processing
//  * - Multiple users independent calculation
//  * - Edge cases and error conditions
//  */
// public class TotalScoreTimeRangeBoundedPrecedingFunctionTest {

//     private static final int WINDOW_SIZE_MINUTES = 5; // 5 minutes window
//     private static final long PREVIOUS_SCORE_TTL_MINUTES = 60; // 1 hour TTL
    
//     private OneInputStreamOperatorTestHarness<Score, Score> testHarness;
//     private TotalScoreTimeRangeBoundedPrecedingFunction function;
//     private AbstractKeyedStateBackend<?> stateBackend;
//     @Before
//     public void setupTestHarness() throws Exception {
//         // Create the function with 5-minute window and 1-hour TTL
//         function = new TotalScoreTimeRangeBoundedPrecedingFunction(WINDOW_SIZE_MINUTES, PREVIOUS_SCORE_TTL_MINUTES);
        
//         // Create the test harness using ProcessFunctionTestHarnesses
//         testHarness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
//             function, 
//             Score::getId,  // Key selector
//             BasicTypeInfo.STRING_TYPE_INFO
//         );
        
//         // Open the test harness
//         testHarness.open();

//         stateBackend = (AbstractKeyedStateBackend<?>) testHarness.getOperator().getKeyedStateBackend();

//         assertThat(stateBackend.numKeyValueStateEntries())
//                 .as("Initial state is not empty")
//                 .isEqualTo(0);
//     }

//     @After
//     public void cleanup() throws Exception {
//         if (testHarness != null) {
//             testHarness.close();
//         }
//     }

//     /**
//      * Helper method to get state value using reflection
//      */
//     @SuppressWarnings("unchecked")
//     private <T> T getStateValue(String stateName, Class<T> expectedType) throws Exception {
//         Field field = function.getClass().getDeclaredField(stateName);
//         field.setAccessible(true);
//         Object state = field.get(function);
        
//         if (state instanceof ValueState) {
//             return ((ValueState<T>) state).value();
//         }
//         return null;
//     }

//     /**
//      * Helper method to get map state entries
//      */
//     @SuppressWarnings("unchecked")
//     private Map<Long, List<Score>> getInputStateEntries() throws Exception {
//         Field field = function.getClass().getDeclaredField("inputState");
//         field.setAccessible(true);
//         MapState<Long, List<Score>> inputState = (MapState<Long, List<Score>>) field.get(function);
        
//         Map<Long, List<Score>> result = new java.util.HashMap<>();
//         for (Map.Entry<Long, List<Score>> entry : inputState.entries()) {
//             result.put(entry.getKey(), entry.getValue());
//         }
//         return result;
//     }

//     @Test
//     public void testBasicScoreAccumulation() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
        
//         // Process first score
//         testHarness.processElement(new Score(userId, 10.0, baseTime), baseTime);
//         testHarness.processWatermark(baseTime);
        
//         // Verify output
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output).hasSize(1);
//         Score result1 = output.get(0);
//         assertEquals(userId, result1.getId());
//         assertEquals(10.0, result1.getScore(), 0.001);
//         assertEquals(0.0, result1.getPreviousScore(), 0.001); // First score, no previous
        
      
        
//         // Process second score
//         long time2 = baseTime + 60000L; // 1 minute later
//         testHarness.processElement(new Score(userId, 15.0, time2), time2);
//         testHarness.processWatermark(time2);
        
//         // Verify accumulated output
//         List<Score> output2 = testHarness.extractOutputValues();
//         assertThat(output2).hasSize(2);
//         Score result2 = output2.get(1);
//         assertEquals(userId, result2.getId());
//         assertEquals(25.0, result2.getScore(), 0.001); // 10 + 15
//         assertEquals(10.0, result2.getPreviousScore(), 0.001); // Previous total was 10
//     }

//     @Test
//     public void testScoreRetractionOutsideWindow() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
//         long windowSizeMs = WINDOW_SIZE_MINUTES * 60 * 1000L; // 5 minutes in ms
        
//         // Process first score
//         testHarness.processElement(new Score(userId, 10.0, baseTime), baseTime);
//         testHarness.processWatermark(baseTime);
        
//         // Process second score within window
//         long time2 = baseTime + 120000L; // 2 minutes later
//         testHarness.processElement(new Score(userId, 20.0, time2), time2);
//         testHarness.processWatermark(time2);
        
//         // Process third score that should cause retraction of first score
//         long time3 = baseTime + windowSizeMs + 60000L; // 6 minutes after first score
//         testHarness.processElement(new Score(userId, 30.0, time3), time3);
//         testHarness.processWatermark(time3);
        
//         // Verify that first score (10.0) is retracted
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output).hasSize(3);
        
//         Score result1 = output.get(0);
//         assertEquals(10.0, result1.getScore(), 0.001);
        
//         Score result2 = output.get(1);
//         assertEquals(30.0, result2.getScore(), 0.001); // 10 + 20
        
//         Score result3 = output.get(2);
//         assertEquals(50.0, result3.getScore(), 0.001); // 20 + 30 (first score retracted)
//     }

//     @Test
//     public void testStateCleanup() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
//         long windowSizeMs = WINDOW_SIZE_MINUTES * 60 * 1000L;
        
//         // Process some scores
//         testHarness.processElement(new Score(userId, 10.0, baseTime), baseTime);
//         testHarness.processElement(new Score(userId, 20.0, baseTime + 60000L), baseTime + 60000L);
        
//         testHarness.processWatermark(baseTime);
//         testHarness.processWatermark(baseTime + 60000L);
        
//         // Verify that scores are processed
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output).hasSize(2);
//         // Cleanup happens at timestamp + precedingOffset * 1.5 + 1
//         long cleanupTime = baseTime + (long)(windowSizeMs * 1.5) + 1;
//         assertThat(getStateValue("cleanupTsState", Long.class)).isEqualTo(cleanupTime);
//         testHarness.processWatermark(cleanupTime);
//          // at this moment the function should have cleaned up states
//          assertThat(stateBackend.numKeyValueStateEntries())
//          .as("State has not been cleaned up")
//          .isEqualTo(1); // cleanup all state except previousScoreState
        
//         // After cleanup, processing new elements should work without issues
//         // This indirectly tests that state cleanup worked
//         testHarness.processElement(new Score(userId, 5.0, cleanupTime + 1000L), cleanupTime + 1000L);
//         testHarness.processWatermark(cleanupTime + 1000L);
        
//         // Should produce new output
//         List<Score> outputAfterCleanup = testHarness.extractOutputValues();
        
//         assertEquals(5.0, outputAfterCleanup.get(2).getScore(), 0.001);
//     }

//     @Test
//     public void testLateDataHandling() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
        
//         // Process first score
//         testHarness.processElement(new Score(userId, 10.0, baseTime), baseTime);
//         testHarness.processWatermark(baseTime);
        
//         // Process second score
//         long time2 = baseTime + 60000L;
//         testHarness.processElement(new Score(userId, 20.0, time2), time2);
//         testHarness.processWatermark(time2);
        
//         // Process late data (timestamp before the last processed)
//         long lateTime = baseTime + 30000L; // 30 seconds after first, but before second
//         testHarness.processElement(new Score(userId, 5.0, lateTime), lateTime);
        
//         // Late data should not trigger new output
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output.get(1).getScore()).isEqualTo(30.0);
//         assertThat(output).hasSize(2); // No new output from late data
       
//     }

//     @Test
//     public void testMultipleUsersIndependentCalculation() throws Exception {
//         long baseTime = 1000L;
        
//         // Process scores for user1
//         testHarness.processElement(new Score("user1", 10.0, baseTime), baseTime);
//         testHarness.processElement(new Score("user1", 20.0, baseTime + 60000L), baseTime + 60000L);
        
//         // Process scores for user2
//         testHarness.processElement(new Score("user2", 5.0, baseTime), baseTime);
//         testHarness.processElement(new Score("user2", 15.0, baseTime + 60000L), baseTime + 60000L);
        
//         // Process watermarks to trigger calculations
//         testHarness.processWatermark(baseTime);
//         testHarness.processWatermark(baseTime + 60000L);
        
//         // Verify independent calculations
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output).hasSize(4);
        
//         // Group by user ID
//         Map<String, List<Score>> outputByUser = output.stream()
//             .collect(Collectors.groupingBy(Score::getId));
        
//         // Verify user1 calculations
//         List<Score> user1Scores = outputByUser.get("user1");
//         assertThat(user1Scores).hasSize(2);
//         assertThat(user1Scores.stream().mapToDouble(Score::getScore))
//             .containsExactlyInAnyOrder(10.0, 30.0);
        
//         // Verify user2 calculations  
//         List<Score> user2Scores = outputByUser.get("user2");
//         assertThat(user2Scores).hasSize(2);
//         assertThat(user2Scores.stream().mapToDouble(Score::getScore))
//             .containsExactlyInAnyOrder(5.0, 20.0);
//     }

//     @Test
//     public void testSameTimestampMultipleScores() throws Exception {
//         String userId = "user1";
//         long timestamp = 1000L;
        
//         // Process multiple scores with same timestamp
//         testHarness.processElement(new Score(userId, 10.0, timestamp), timestamp);
//         testHarness.processElement(new Score(userId, 15.0, timestamp), timestamp);
//         testHarness.processElement(new Score(userId, 5.0, timestamp), timestamp);
        
//         testHarness.processWatermark(timestamp);
        
//         // Should produce one output with sum of all scores
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output).hasSize(1);
//         assertEquals(30.0, output.get(0).getScore(), 0.001); // 10 + 15 + 5
//     }


//     @Test
//     public void testZeroScoreHandling() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
        
//         // Process zero score
//         testHarness.processElement(new Score(userId, 0.0, baseTime), baseTime);
//         testHarness.processWatermark(baseTime);
        
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output).hasSize(1);
//         assertEquals(0.0, output.get(0).getScore(), 0.001);
//         assertEquals(0.0, output.get(0).getPreviousScore(), 0.001);
//     }

//     @Test
//     public void testNegativeScoreHandling() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
        
//         // Process positive score first
//         testHarness.processElement(new Score(userId, 10.0, baseTime), baseTime);
//         testHarness.processWatermark(baseTime);
        
//         // Process negative score
//         long time2 = baseTime + 60000L;
//         testHarness.processElement(new Score(userId, -5.0, time2), time2);
//         testHarness.processWatermark(time2);
        
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output).hasSize(2);
        
//         Score result1 = output.get(0);
//         assertEquals(10.0, result1.getScore(), 0.001);
//         assertEquals(0.0, result1.getPreviousScore(), 0.001);
        
//         Score result2 = output.get(1);
//         assertEquals(5.0, result2.getScore(), 0.001); // 10 + (-5)
//         assertEquals(10.0, result2.getPreviousScore(), 0.001);
//     }

//     @Test
//     public void testLargeScoreValues() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
//         double largeScore = Double.MAX_VALUE / 2; // Use large but safe values
        
//         // Process large score
//         testHarness.processElement(new Score(userId, largeScore, baseTime), baseTime);
//         testHarness.processWatermark(baseTime);
        
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output).hasSize(1);
//         assertEquals(largeScore, output.get(0).getScore(), 0.001);
//     }


//     @Test
//     public void testWindowBoundaryEdgeCase() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
//         long windowSizeMs = WINDOW_SIZE_MINUTES * 60 * 1000L;
        
//         // Process score at the exact window boundary
//         long boundaryTime = baseTime + windowSizeMs +1;
//         testHarness.processElement(new Score(userId, 10.0, baseTime), baseTime);
//         testHarness.processElement(new Score(userId, 20.0, boundaryTime), boundaryTime);
        
//         testHarness.processWatermark(baseTime);
//         testHarness.processWatermark(boundaryTime);
        
//         List<Score> output = testHarness.extractOutputValues();
//         assertThat(output).hasSize(2);
        
//         // First score should be 10.0
//         assertEquals(10.0, output.get(0).getScore(), 0.001);
        
//         // Second score should be 20.0 (first score should be retracted as it's exactly at boundary)
//         assertEquals(20.0, output.get(1).getScore(), 0.001);
//     }



//     @Test
//     public void testDetailedStateVerification() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
        

//         // Process first score
//         testHarness.processElement(new Score(userId, 10.0, baseTime), baseTime);
        
//         // Before watermark, state should have entries (inputState stores the data)
//         assertThat(stateBackend.numKeyValueStateEntries())
//                 .as("Should have state entries after processing element but before watermark")
//                 .isGreaterThan(0);
        
//         testHarness.processWatermark(baseTime);
        
//         // State should still exist after processing
//         assertThat(stateBackend.numKeyValueStateEntries())
//                 .as("Should maintain state entries after first watermark")
//                 .isGreaterThan(0);
        
//         // Process second score
//         long time2 = baseTime + 60000L; // 1 minute later
//         testHarness.processElement(new Score(userId, 15.0, time2), time2);
//         testHarness.processWatermark(time2);
        
//         // State should still exist after second processing
//         assertThat(stateBackend.numKeyValueStateEntries())
//                 .as("Should maintain state entries after second watermark")
//                 .isGreaterThan(0);
        
//         // Process third score to test retraction
//         long time3 = baseTime + 120000L; // 2 minutes after first score
//         testHarness.processElement(new Score(userId, 5.0, time3), time3);
//         testHarness.processWatermark(time3);
        
//         // State should still exist
//         assertThat(stateBackend.numKeyValueStateEntries())
//                 .as("Should maintain state entries after third watermark")
//                 .isGreaterThan(0);
//     }

//     @Test
//     public void testDetailedStateValuesVerification() throws Exception {
//         String userId = "user1";
//         long baseTime = 1000L;
        
//         // Process first score
//         testHarness.processElement(new Score(userId, 10.0, baseTime), baseTime);
        
//         // Before watermark, inputState should contain the data
//         Map<Long, List<Score>> inputStateEntries = getInputStateEntries();
//         assertThat(inputStateEntries)
//                 .as("inputState should contain data after processing element")
//                 .hasSize(1);
//         assertThat(inputStateEntries.get(baseTime))
//                 .as("inputState should contain the score at correct timestamp")
//                 .hasSize(1);
//         assertThat(inputStateEntries.get(baseTime).get(0).getScore())
//                 .as("Score in inputState should match input")
//                 .isEqualTo(10.0);
        
//         // Other states should still be null before watermark
//         assertThat(getStateValue("totalScoreState", Double.class))
//                 .as("totalScoreState should still be null before watermark")
//                 .isNull();
//         assertThat(getStateValue("previousScoreState", Double.class))
//                 .as("previousScoreState should still be null before watermark")
//                 .isNull();
        
//         Long cleanupTs = getStateValue("cleanupTsState", Long.class);
//         long expectedCleanupTs = baseTime + (long)(WINDOW_SIZE_MINUTES * 60 * 1000L * 1.5) + 1;
//         assertThat(cleanupTs)
//                 .as("cleanupTsState should be set to expected cleanup time")
//                 .isEqualTo(expectedCleanupTs);        

//         testHarness.processWatermark(baseTime);
        
//         // After first watermark, states should be updated
//         assertThat(getStateValue("totalScoreState", Double.class))
//                 .as("totalScoreState should be 10.0 after first watermark")
//                 .isEqualTo(10.0);
//         assertThat(getStateValue("previousScoreState", Double.class))
//                 .as("previousScoreState should be 10.0 after first watermark")
//                 .isEqualTo(10.0);
//         assertThat(getStateValue("lastTriggeringTsState", Long.class))
//                 .as("lastTriggeringTsState should be updated after first watermark")
//                 .isEqualTo(baseTime);
        
//         // Process second score
//         long time2 = baseTime + 60000L; // 1 minute later
//         testHarness.processElement(new Score(userId, 15.0, time2), time2);
        
//         // inputState should now contain both timestamps
//         inputStateEntries = getInputStateEntries();
//         assertThat(inputStateEntries)
//                 .as("inputState should contain data for both timestamps")
//                 .hasSize(2);
//         assertThat(inputStateEntries.get(time2))
//                 .as("inputState should contain the second score")
//                 .hasSize(1);
//         assertThat(inputStateEntries.get(time2).get(0).getScore())
//                 .as("Second score in inputState should match input")
//                 .isEqualTo(15.0);
        
//         testHarness.processWatermark(time2);
        
//         // After second watermark, states should be updated
//         assertThat(getStateValue("totalScoreState", Double.class))
//                 .as("totalScoreState should be 25.0 after second watermark")
//                 .isEqualTo(25.0);
//         assertThat(getStateValue("previousScoreState", Double.class))
//                 .as("previousScoreState should be 25.0 after second watermark")
//                 .isEqualTo(25.0);
//         assertThat(getStateValue("lastTriggeringTsState", Long.class))
//                 .as("lastTriggeringTsState should be updated after second watermark")
//                 .isEqualTo(time2);
        
//         // Process third score to test retraction
//         long time3 = baseTime + 60000L*6; // 6 minutes after first score
//         testHarness.processElement(new Score(userId, 5.0, time3), time3);
//         testHarness.processWatermark(time3);
        
//         // After third watermark with retraction
//         assertThat(getStateValue("totalScoreState", Double.class))
//                 .as("totalScoreState should be 20.0 after retraction (15 + 5, first score 10 retracted)")
//                 .isEqualTo(20.0);
//         assertThat(getStateValue("previousScoreState", Double.class))
//                 .as("previousScoreState should be 20.0 after third watermark")
//                 .isEqualTo(20.0);
//         assertThat(getStateValue("lastTriggeringTsState", Long.class))
//                 .as("lastTriggeringTsState should be updated after third watermark")
//                 .isEqualTo(time3);
        
//         // inputState should only contain recent data (first score should be removed)
//         inputStateEntries = getInputStateEntries();
//         assertThat(inputStateEntries)
//                 .as("inputState should only contain recent data after retraction")
//                 .hasSize(2); // time2 and time3, baseTime should be removed
//         assertThat(inputStateEntries.containsKey(baseTime))
//                 .as("First timestamp should be removed from inputState")
//                 .isFalse();
//         assertThat(inputStateEntries.containsKey(time2))
//                 .as("Second timestamp should still be in inputState")
//                 .isTrue();
//         assertThat(inputStateEntries.containsKey(time3))
//                 .as("Third timestamp should be in inputState")
//                 .isTrue();
//     }
// }
