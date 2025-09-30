package jobs.operators;

import jobs.models.Score;
import jobs.models.ScoreChangeEvent;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.HashMap;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.MapState;
import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class RetractableTopNFunctionTest {

    private OneInputStreamOperatorTestHarness<Score, ScoreChangeEvent> testHarness;
    
    @SuppressWarnings("unchecked")
    private <T> T getValueState(String fieldName, Class<T> expected) throws Exception {
        Field f = testHarness.getOperator().getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        Object state = f.get(testHarness.getOperator());
        if (state instanceof ValueState) {
            return ((ValueState<T>) state).value();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Map<Double, List<Score>> snapshotDataState() throws Exception {
        Field f = testHarness.getOperator().getClass().getDeclaredField("dataState");
        f.setAccessible(true);
        MapState<Double, List<Score>> ms = (MapState<Double, List<Score>>) f.get(testHarness.getOperator());
        Map<Double, List<Score>> out = new HashMap<>();
        for (Map.Entry<Double, List<Score>> e : ms.entries()) {
            out.put(e.getKey(), e.getValue());
        }
        return out;
    }

    @Before
    public void setUp() throws Exception {
        // topN=3, ttlMinutes=60, cleanupIntervalMinutes=5 (values not critical for core behaviors below)
        RetractableTopNFunction fn = new RetractableTopNFunction(3, 60, 5);
        testHarness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                fn,
                Score::getId,
                BasicTypeInfo.STRING_TYPE_INFO
        );
        testHarness.open();
    }

    @After
    public void tearDown() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

//user submission : 
// 1. không có prevScore và điểm outtopN
// 2. không có prevScore và điểm trong topN
// 3. có prev Score không trong top N và score trong topN
// 4. có prevScore trong top N và score trong top N
    @Test
    public void testFourInsertsTopNTransitions() throws Exception {
        long t0 = 1_000L;
        long t1 = 2_000L;
        long t2 = 3_000L;
        long t3 = 4_000L;

        // Insert three users that should all be in Top-3
        testHarness.processElement(new Score("u1", 30.0, 0.0, t0), t0); // rank 1
        testHarness.processElement(new Score("u2", 25.0, 0.0, t1), t1); // rank 2
        testHarness.processElement(new Score("u3", 20.0, 0.0, t2), t2); // rank 3

        // Insert a fourth user with score between existing ranks (will enter Top-3)
        // Expect: DELETE u3 (20.0) then INSERT u4 (27.0)
        testHarness.processElement(new Score("u4", 27.0, 0.0, t3), t3);

        List<ScoreChangeEvent> out = testHarness.extractOutputValues();
        // First three should be INSERTs
        assertThat(out.size()).isEqualTo(5);
        assertThat(out.get(0).isInsert()).isTrue();
        assertThat(out.get(1).isInsert()).isTrue();
        assertThat(out.get(2).isInsert()).isTrue();

        // The last two events should reflect Top-3 transition: DELETE (u3,20) then INSERT (u4,27)
        ScoreChangeEvent penultimate = out.get(out.size() - 2);
        ScoreChangeEvent last = out.get(out.size() - 1);

        assertThat(penultimate.isDelete()).isTrue();
        assertEquals("u3", penultimate.getScore().getId());
        assertEquals(20.0, penultimate.getScore().getScore(), 0.001);

        assertThat(last.isInsert()).isTrue();
        assertEquals("u4", last.getScore().getId());
        assertEquals(27.0, last.getScore().getScore(), 0.001);

        // State assertions
        // dataState should contain scores 30.0, 27.0, 25.0 only
        Map<Double, List<Score>> dataSnapshot = snapshotDataState();
        assertThat(dataSnapshot.keySet()).containsExactlyInAnyOrder(30.0, 27.0, 25.0);
        // treeMap counts should be 1 for each of those keys
        @SuppressWarnings("unchecked")
        SortedMap<Double, Long> tree = (SortedMap<Double, Long>) (SortedMap<?, ?>) getValueState("treeMap", SortedMap.class);
        assertThat(tree.get(30.0)).isEqualTo(1L);
        assertThat(tree.get(27.0)).isEqualTo(1L);
        assertThat(tree.get(25.0)).isEqualTo(1L);
        assertThat(tree.containsKey(20.0)).isFalse();
        // timerRegistered should be true after processing
        Boolean timerReg = getValueState("timerRegistered", Boolean.class);
        assertThat(timerReg).isTrue();
    }

    @Test
    public void testUpdateScoreEmitsInsert() throws Exception {
        long t0 = 1_000L;
        long t1 = 2_000L;
        long t2 = 3_000L;
        // First insert
        Score s1 = new Score("u1", 10.0, 0.0, t0);
        testHarness.processElement(s1, t0);

        Score s2 = new Score("u2", 50.0, 10.0, t1);
        testHarness.processElement(s2, t1);

        // Update to a different score -> should retract previous and insert new
        Score s3 = new Score("u1", 20.0, 10.0, t1);
        testHarness.processElement(s3, t2);

        List<ScoreChangeEvent> out = testHarness.extractOutputValues();
        // Expect: INSERT(10), INSERT(50), INSERT(20) 
        assertThat(out.size()).isEqualTo(3);

        ScoreChangeEvent firstScoreChangeEvent = out.get(0);
        assertThat(firstScoreChangeEvent.isInsert()).isTrue();
        assertThat(firstScoreChangeEvent.getScore().getId()).isEqualTo("u1");
        assertThat(firstScoreChangeEvent.getScore().getScore()).isEqualTo(10.0);

        ScoreChangeEvent last = out.get(out.size() - 1);
        assertThat(last.isInsert()).isTrue();
        assertEquals(20.0, last.getScore().getScore(), 0.001);
        
        // State assertions: previous 10.0 should be retracted; 20.0 present
        Map<Double, List<Score>> dataSnapshot = snapshotDataState();
        assertThat(dataSnapshot.containsKey(10.0)).isFalse();
        assertThat(dataSnapshot.containsKey(20.0)).isTrue();
        @SuppressWarnings("unchecked")
        SortedMap<Double, Long> tree = (SortedMap<Double, Long>) (SortedMap<?, ?>) getValueState("treeMap", SortedMap.class);
        assertThat(tree.get(20.0)).isEqualTo(1L);
        assertThat(tree.get(50.0)).isEqualTo(1L);
        Boolean timerReg = getValueState("timerRegistered", Boolean.class);
        assertThat(timerReg).isTrue();
    }

    @Test
    public void testPrevNotInTopN_NewScoreEntersTopN() throws Exception {
        long t0 = 1_000L;
        long t1 = 2_000L;
        long t2 = 3_000L;
        long t3 = 4_000L;
        long t4 = 5_000L;

        // Seed Top-3: u1=30, u2=25, u3=20
        testHarness.processElement(new Score("u1", 30.0, 0.0, t0), t0);
        testHarness.processElement(new Score("u2", 25.0, 0.0, t1), t1);
        testHarness.processElement(new Score("u3", 20.0, 0.0, t2), t2);
        assertThat(testHarness.extractOutputValues()).hasSize(3);

        // User ux previously had low score 5.0 (not in Top-3) — should not emit
        testHarness.processElement(new Score("ux", 5.0, 0.0, t3), t3);
        assertThat(testHarness.extractOutputValues()).hasSize(3);

        // Now ux improves to 27.0 with prevScore=5.0; should emit DELETE(u3,20) then INSERT(ux,27)
        testHarness.processElement(new Score("ux", 27.0, 5.0, t4), t4);

        List<ScoreChangeEvent> out = testHarness.extractOutputValues();
        assertThat(out).hasSize(5);
        ScoreChangeEvent penultimate = out.get(out.size() - 2);
        ScoreChangeEvent last = out.get(out.size() - 1);
        assertThat(penultimate.isDelete()).isTrue();
        assertEquals("u3", penultimate.getScore().getId());
        assertEquals(20.0, penultimate.getScore().getScore(), 0.001);
        assertThat(last.isInsert()).isTrue();
        assertEquals("ux", last.getScore().getId());
        assertEquals(27.0, last.getScore().getScore(), 0.001);

        // State assertions
        Map<Double, List<Score>> dataSnapshot = snapshotDataState();
        assertThat(dataSnapshot.keySet()).containsExactlyInAnyOrder(30.0, 27.0, 25.0);
        @SuppressWarnings("unchecked")
        SortedMap<Double, Long> tree = (SortedMap<Double, Long>) (SortedMap<?, ?>) getValueState("treeMap", SortedMap.class);
        assertThat(tree.get(30.0)).isEqualTo(1L);
        assertThat(tree.get(27.0)).isEqualTo(1L);
        assertThat(tree.get(25.0)).isEqualTo(1L);
        assertThat(tree.containsKey(20.0)).isFalse();
        assertThat(tree.containsKey(5.0)).isFalse();
        Boolean timerReg = getValueState("timerRegistered", Boolean.class);
        assertThat(timerReg).isTrue();
    }

    @Test
    public void testNoPrevScoreAndOutOfTopNProducesNoOutput() throws Exception {
        long t0 = 1_000L;
        long t1 = 2_000L;
        long t2 = 3_000L;
        long t3 = 4_000L;

        // Seed Top-3
        testHarness.processElement(new Score("u1", 30.0, 0.0, t0), t0);
        testHarness.processElement(new Score("u2", 25.0, 0.0, t1), t1);
        testHarness.processElement(new Score("u3", 20.0, 0.0, t2), t2);
        assertThat(testHarness.extractOutputValues()).hasSize(3);

        // New user u4 with score below Top-3 and no prevScore — should not emit
        testHarness.processElement(new Score("u4", 19.0, 0.0, t3), t3);
        assertThat(testHarness.extractOutputValues()).hasSize(3);

        // State assertions: Top-3 unchanged
        Map<Double, List<Score>> dataSnapshot = snapshotDataState();
        assertThat(dataSnapshot.keySet()).containsExactlyInAnyOrder(30.0, 25.0, 20.0);
        @SuppressWarnings("unchecked")
        SortedMap<Double, Long> tree = (SortedMap<Double, Long>) (SortedMap<?, ?>) getValueState("treeMap", SortedMap.class);
        assertThat(tree.get(30.0)).isEqualTo(1L);
        assertThat(tree.get(25.0)).isEqualTo(1L);
        assertThat(tree.get(20.0)).isEqualTo(1L);
        assertThat(tree.get(19.0)).isEqualTo(1L);
        Boolean timerReg = getValueState("timerRegistered", Boolean.class);
        assertThat(timerReg).isTrue();
    }
}
