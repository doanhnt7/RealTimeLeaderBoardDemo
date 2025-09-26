/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jobs.operators;

import jobs.models.Score;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Comprehensive tests for {@link TotalScoreTimeRangeBoundedPrecedingFunction}.
 * 
 * Tests include:
 * - Correct score calculation (accumulation and retraction)
 * - State cleanup functionality
 * - Late data handling
 * - Timer registration and processing
 */
public class TotalScoreTimeRangeBoundedPrecedingFunctionTest {

    private static final int WINDOW_SIZE_MINUTES = 5; // 5 minutes window
    private static final long PREVIOUS_SCORE_TTL_MINUTES = 60; // 1 hour TTL
    
    private KeyedOneInputStreamOperatorTestHarness<String, Score, Score> testHarness;
    private TotalScoreTimeRangeBoundedPrecedingFunction function;

    /**
     * Key selector to extract user ID from Score objects
     */
    private static class ScoreKeySelector implements KeySelector<Score, String> {
        @Override
        public String getKey(Score score) throws Exception {
            return score.getId();
        }
    }

    @Before
    public void setupTestHarness() throws Exception {
        // Create the function with 5-minute window and 1-hour TTL
        function = new TotalScoreTimeRangeBoundedPrecedingFunction(WINDOW_SIZE_MINUTES, PREVIOUS_SCORE_TTL_MINUTES);
        
        // Wrap the function in a KeyedProcessOperator
        KeyedProcessOperator<String, Score, Score> operator = new KeyedProcessOperator<>(function);
        
        // Create the test harness with key selector
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
            operator, 
            new ScoreKeySelector(), 
            Types.STRING
        );
        
        // Open the test harness
        testHarness.open();
    }

    @After
    public void cleanup() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

    @Test
    public void testBasicScoreAccumulation() throws Exception {
        String userId = "user1";
        long baseTime = 1000L;
        
        // Process first score
        testHarness.processElement(new StreamRecord<>(new Score(userId, 10.0, baseTime), baseTime));
        testHarness.processWatermark(baseTime);
        
        // Verify output
        @SuppressWarnings("unchecked")
        List<StreamRecord<Score>> output = (List<StreamRecord<Score>>) (List<?>) testHarness.extractOutputStreamRecords();
        assertThat(output).hasSize(1);
        Score result1 = output.get(0).getValue();
        assertEquals(userId, result1.getId());
        assertEquals(10.0, result1.getScore(), 0.001);
        assertEquals(0.0, result1.getPreviousScore(), 0.001); // First score, no previous
        
        // Process second score
        long time2 = baseTime + 60000L; // 1 minute later
        testHarness.processElement(new StreamRecord<>(new Score(userId, 15.0, time2), time2));
        testHarness.processWatermark(time2);
        
        // Verify accumulated output
        @SuppressWarnings("unchecked")
        List<StreamRecord<Score>> output2 = (List<StreamRecord<Score>>) (List<?>) testHarness.extractOutputStreamRecords();
        assertThat(output2).hasSize(2);
        Score result2 = output2.get(1).getValue();
        assertEquals(userId, result2.getId());
        assertEquals(25.0, result2.getScore(), 0.001); // 10 + 15
        assertEquals(10.0, result2.getPreviousScore(), 0.001); // Previous total was 10
    }

    @Test
    public void testScoreRetractionOutsideWindow() throws Exception {
        String userId = "user1";
        long baseTime = 1000L;
        long windowSizeMs = WINDOW_SIZE_MINUTES * 60 * 1000L; // 5 minutes in ms
        
        // Process first score
        testHarness.processElement(new StreamRecord<>(new Score(userId, 10.0, baseTime), baseTime));
        testHarness.processWatermark(baseTime);
        
        // Process second score within window
        long time2 = baseTime + 120000L; // 2 minutes later
        testHarness.processElement(new StreamRecord<>(new Score(userId, 20.0, time2), time2));
        testHarness.processWatermark(time2);
        
        // Process third score that should cause retraction of first score
        long time3 = baseTime + windowSizeMs + 60000L; // 6 minutes after first score
        testHarness.processElement(new StreamRecord<>(new Score(userId, 30.0, time3), time3));
        testHarness.processWatermark(time3);
        
        // Verify that first score (10.0) is retracted
        @SuppressWarnings("unchecked")
        List<StreamRecord<Score>> output = (List<StreamRecord<Score>>) (List<?>) testHarness.extractOutputStreamRecords();
        assertThat(output).hasSize(3);
        
        Score result1 = output.get(0).getValue();
        assertEquals(10.0, result1.getScore(), 0.001);
        
        Score result2 = output.get(1).getValue();
        assertEquals(30.0, result2.getScore(), 0.001); // 10 + 20
        
        Score result3 = output.get(2).getValue();
        assertEquals(50.0, result3.getScore(), 0.001); // 20 + 30 (first score retracted)
    }

    @Test
    public void testStateCleanup() throws Exception {
        String userId = "user1";
        long baseTime = 1000L;
        long windowSizeMs = WINDOW_SIZE_MINUTES * 60 * 1000L;
        
        KeyedProcessOperator<String, Score, Score> operator = 
            (KeyedProcessOperator<String, Score, Score>) testHarness.getOperator();
        AbstractKeyedStateBackend stateBackend = 
            (AbstractKeyedStateBackend) operator.getKeyedStateBackend();
        
        // Initially, state should be empty
        assertThat(stateBackend.numKeyValueStateEntries())
            .as("Initial state should be empty")
            .isEqualTo(0);
        
        // Process some scores
        testHarness.processElement(new StreamRecord<>(new Score(userId, 10.0, baseTime), baseTime));
        testHarness.processElement(new StreamRecord<>(new Score(userId, 20.0, baseTime + 60000L), baseTime + 60000L));
        
        testHarness.processWatermark(baseTime);
        testHarness.processWatermark(baseTime + 60000L);
        
        // State should have entries now
        assertThat(stateBackend.numKeyValueStateEntries())
            .as("State should contain entries after processing")
            .isGreaterThan(0);
        
        // Advance time significantly to trigger cleanup
        // Cleanup happens at timestamp + precedingOffset * 1.5 + 1
        long cleanupTime = baseTime + (long)(windowSizeMs * 1.5) + 60000L + 1;
        testHarness.processWatermark(cleanupTime);
        
        // State should be cleaned up
        assertThat(stateBackend.numKeyValueStateEntries())
            .as("State should be cleaned up after cleanup time")
            .isEqualTo(0);
    }

    @Test
    public void testLateDataHandling() throws Exception {
        String userId = "user1";
        long baseTime = 1000L;
        
        // Process first score
        testHarness.processElement(new StreamRecord<>(new Score(userId, 10.0, baseTime), baseTime));
        testHarness.processWatermark(baseTime);
        
        // Process second score
        long time2 = baseTime + 60000L;
        testHarness.processElement(new StreamRecord<>(new Score(userId, 20.0, time2), time2));
        testHarness.processWatermark(time2);
        
        // Process late data (timestamp before the last processed)
        long lateTime = baseTime + 30000L; // 30 seconds after first, but before second
        testHarness.processElement(new StreamRecord<>(new Score(userId, 5.0, lateTime), lateTime));
        
        // Late data should not trigger new output
        @SuppressWarnings("unchecked")
        List<StreamRecord<Score>> output = (List<StreamRecord<Score>>) (List<?>) testHarness.extractOutputStreamRecords();
        assertThat(output).hasSize(2); // No new output from late data
        
        // Verify metric counter for late records would be incremented
        // (In real scenario, we would check the metric, but test harness doesn't expose metrics easily)
    }

    @Test
    public void testMultipleUsersIndependentCalculation() throws Exception {
        long baseTime = 1000L;
        
        // Process scores for user1
        testHarness.processElement(new StreamRecord<>(new Score("user1", 10.0, baseTime), baseTime));
        testHarness.processElement(new StreamRecord<>(new Score("user1", 20.0, baseTime + 60000L), baseTime + 60000L));
        
        // Process scores for user2
        testHarness.processElement(new StreamRecord<>(new Score("user2", 5.0, baseTime), baseTime));
        testHarness.processElement(new StreamRecord<>(new Score("user2", 15.0, baseTime + 60000L), baseTime + 60000L));
        
        // Process watermarks to trigger calculations
        testHarness.processWatermark(baseTime);
        testHarness.processWatermark(baseTime + 60000L);
        
        // Verify independent calculations
        @SuppressWarnings("unchecked")
        List<StreamRecord<Score>> output = (List<StreamRecord<Score>>) (List<?>) testHarness.extractOutputStreamRecords();
        assertThat(output).hasSize(4);
        
        // Find results for each user
        Score user1Result1 = null, user1Result2 = null, user2Result1 = null, user2Result2 = null;
        
        for (StreamRecord<Score> record : output) {
            Score score = record.getValue();
            if ("user1".equals(score.getId())) {
                if (score.getScore() == 10.0) {
                    user1Result1 = score;
                } else if (score.getScore() == 30.0) {
                    user1Result2 = score;
                }
            } else if ("user2".equals(score.getId())) {
                if (score.getScore() == 5.0) {
                    user2Result1 = score;
                } else if (score.getScore() == 20.0) {
                    user2Result2 = score;
                }
            }
        }
        
        // Verify user1 calculations
        assertThat(user1Result1).isNotNull();
        assertThat(user1Result2).isNotNull();
        assertEquals(10.0, user1Result1.getScore(), 0.001);
        assertEquals(30.0, user1Result2.getScore(), 0.001);
        
        // Verify user2 calculations
        assertThat(user2Result1).isNotNull();
        assertThat(user2Result2).isNotNull();
        assertEquals(5.0, user2Result1.getScore(), 0.001);
        assertEquals(20.0, user2Result2.getScore(), 0.001);
    }

    @Test
    public void testSameTimestampMultipleScores() throws Exception {
        String userId = "user1";
        long timestamp = 1000L;
        
        // Process multiple scores with same timestamp
        testHarness.processElement(new StreamRecord<>(new Score(userId, 10.0, timestamp), timestamp));
        testHarness.processElement(new StreamRecord<>(new Score(userId, 15.0, timestamp), timestamp));
        testHarness.processElement(new StreamRecord<>(new Score(userId, 5.0, timestamp), timestamp));
        
        testHarness.processWatermark(timestamp);
        
        // Should produce one output with sum of all scores
        @SuppressWarnings("unchecked")
        List<StreamRecord<Score>> output = (List<StreamRecord<Score>>) (List<?>) testHarness.extractOutputStreamRecords();
        assertThat(output).hasSize(1);
        assertEquals(30.0, output.get(0).getValue().getScore(), 0.001); // 10 + 15 + 5
    }

    @Test
    public void testTimerRegistration() throws Exception {
        String userId = "user1";
        long baseTime = 1000L;
        
        // Process element to trigger timer registration
        testHarness.processElement(new StreamRecord<>(new Score(userId, 10.0, baseTime), baseTime));
        
        // Verify that timer was registered by checking if watermark triggers processing
        testHarness.processWatermark(baseTime);
        
        @SuppressWarnings("unchecked")
        List<StreamRecord<Score>> output = (List<StreamRecord<Score>>) (List<?>) testHarness.extractOutputStreamRecords();
        assertThat(output).hasSize(1);
        assertEquals(10.0, output.get(0).getValue().getScore(), 0.001);
    }
}
