package flinkfintechpoc.test_jobs;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.time.Duration;

// POJO class cho sự kiện
public class ProcessClearDemo {

	public static class LogEvent {
		public String key;
		public long ts;

		public LogEvent() {}

		public LogEvent(String key, long ts) {
			this.key = key;
			this.ts = ts;
		}

		@Override
		public String toString() {
			return "LogEvent{" + "key='" + key + '\'' + ", ts=" + ts + '}';
		}
	}

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// Tạo dữ liệu test
		DataStream<LogEvent> events = env.fromData(
				new LogEvent("a", 1000L),  // thuộc window [0,5s)
				new LogEvent("a", 2000L),
				new LogEvent("a", 4000L),
				new LogEvent("a", 6000L),  // thuộc window [5s,10s)
				new LogEvent("a", 7000L)
		);

		// Gắn watermark
		DataStream<LogEvent> withWatermarks = events
				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<LogEvent>forBoundedOutOfOrderness(Duration.ZERO)
								.withTimestampAssigner((SerializableTimestampAssigner<LogEvent>) (element, recordTimestamp) -> element.ts)
				);

		// Áp dụng tumbling window 5 giây
		withWatermarks
				.keyBy(e -> e.key)
				.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
				.process(new ProcessWindowFunction<LogEvent, String, String, TimeWindow>() {

					// Window state descriptor
					ValueStateDescriptor<String> windowDescriptor = new ValueStateDescriptor<>(
								"window-info",
								TypeInformation.of(new TypeHint<String>() {}));

					// Global state descriptor
					ValueStateDescriptor<Integer> globalDescriptor = new ValueStateDescriptor<>(
								"global-count",
								TypeInformation.of(new TypeHint<Integer>() {}));

					@Override
					public void process(String key,
									   Context context,
									   Iterable<LogEvent> elements,
									   Collector<String> out) throws Exception {

						ValueState<String> windowInfoState = context.windowState().getState(windowDescriptor);
						ValueState<Integer> globalCountState = context.globalState().getState(globalDescriptor);

						long count = 0;
						for (LogEvent e : elements) {
							count++;
							// Sử dụng biến e để tránh warning
							System.out.println("Processing event: " + e);
						}
						System.out.println(windowInfoState.value());
						// Lưu thông tin window vào state
						String windowInfo = "Window[" + context.window().getStart() + "," +
									context.window().getEnd() + ") processed at " +
									System.currentTimeMillis() + " with count=" + count;
						windowInfoState.update(windowInfo);

						// Cập nhật global state (tổng số event đã process qua tất cả window)
						Integer globalCount = globalCountState.value();
						if (globalCount == null) {
							globalCount = 0;
						}
						globalCount += Integer.parseInt(String.valueOf(count));
						globalCountState.update(globalCount);

						System.out.println(">>> State saved: " + windowInfo);
						System.out.println(">>> Global count (across all windows): " + globalCount);

						out.collect("Window result count=" + count + ", globalCount=" + globalCount);
					}

					@Override
					public void clear(Context context) throws Exception {
						ValueState<String> windowInfoState = context.windowState().getState(windowDescriptor);
						ValueState<Integer> globalCountState = context.globalState().getState(globalDescriptor);

						// Kiểm tra window state trước khi clear
						String savedState = windowInfoState.value();
						System.out.println(">>> clear() called for window: [" +
								context.window().getStart() + "," +
								context.window().getEnd() + ")");
						System.out.println(">>> Window state before clear: " + savedState);

						// Kiểm tra global state (không bị xóa khi window clear)
						Integer globalCount = globalCountState.value();
						System.out.println(">>> Global count in clear(): " + globalCount);

						// In RAM của JVM (TaskManager process) tại thời điểm clear
						Runtime runtime = Runtime.getRuntime();
						long total = runtime.totalMemory();
						long free = runtime.freeMemory();
						long used = total - free;
						System.out.println(String.format(
								">>> JVM Memory at clear - used: %.2f MB, free: %.2f MB, total: %.2f MB",
								used / 1024.0 / 1024.0,
								free / 1024.0 / 1024.0,
								total / 1024.0 / 1024.0));

						// windowInfoState.clear(); // nếu không clear thì windowState sẽ bị stuck và làm phình backend state

						// Kiểm tra state sau khi clear
						// String stateAfterClear = windowInfoState.value();
						// System.out.println(">>> Window state after clear: " + (stateAfterClear != null ? stateAfterClear : "NULL"));
						// System.out.println(">>> Note: windowState sẽ tự động bị xóa khi window kết thúc");
						// System.out.println(">>> Note: globalState sẽ không bị xóa khi window clear");
					}
				})
				.print();

			env.execute("Process vs Clear Demo");
	}
}
