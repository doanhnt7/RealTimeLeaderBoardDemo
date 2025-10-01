#!/usr/bin/env python3
"""
Spark job để xử lý leaderboard với logic tương tự Flink pipeline

Pipeline:
1. Đọc JSONL data từ file
2. Transform thành Score objects với event time
3. Tính tổng điểm trong sliding window (x phút gần nhất)
4. Tính TopN với retraction logic
5. Snapshot mỗi 7 phút theo event time
"""

import sys
import json
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import math

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


@dataclass
class UserData:
    uid: str
    level: int
    team: int
    updatedAt: str
    name: str
    geo: str


@dataclass
class Score:
    id: str
    score: float
    lastUpdateTime: int
    previousScore: Optional[float] = None


@dataclass
class UserTotalScore:
    userId: str
    totalScore: float
    previousTotalScore: float
    lastUpdateTime: int


@dataclass
class LeaderBoardEntry:
    userId: str
    totalScore: float
    rank: int
    lastUpdateTime: int
    snapshotTime: int


@dataclass
class ScoreChangeEvent:
    changeType: str  # "INSERT", "DELETE", "DELETEALL"
    score: Optional[Score] = None


class LeaderBoardProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    def read_data(self, input_path: str) -> DataFrame:
        """Đọc data từ  Parquet file"""
        import os
        
      
        if os.path.exists(input_path):
            print(f"Found Parquet version, reading from: {input_path}")
            return self.spark.read.parquet(input_path)
        else:
            raise ValueError(f"Unsupported file format: {input_path}")
    
    def parse_timestamp(self, timestamp_str: str) -> int:
        """Parse timestamp string to milliseconds"""
        try:
            # Try parsing ISO format
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except:
            # Fallback to current time
            return int(datetime.now().timestamp() * 1000)
    
    def transform_to_scores(self, user_data: DataFrame) -> DataFrame:
        """Transform user data to Score objects"""
        def create_score(row):
            timestamp = self.parse_timestamp(row.updatedAt)
            score = float(row.level)
            
            return {
                'id': row.uid,
                'score': score,
                'lastUpdateTime': timestamp,
                'previousScore': None
            }
        
        return user_data.rdd.map(create_score).toDF()
    
    def calculate_total_scores_in_window(self, scores: DataFrame, window_size_minutes: int) -> DataFrame:
        """Tính tổng điểm trong sliding window - tương tự TotalScoreTimeRangeBoundedPrecedingFunction"""
        window_size_ms = window_size_minutes * 60 * 1000
        
        # Group by user và tính total score cho mỗi timestamp
        def calculate_user_total_scores(user_scores):
            user_id = user_scores[0]
            score_list = sorted(user_scores[1], key=lambda x: x['lastUpdateTime'])
            
            results = []
            for i, current_score in enumerate(score_list):
                current_time = current_score['lastUpdateTime']
                window_start = current_time - window_size_ms
                
                # Lấy tất cả scores trong window
                scores_in_window = [s for s in score_list if s['lastUpdateTime'] > window_start]
                total_score = sum(s['score'] for s in scores_in_window)
                
                # Tính previous total score
                previous_total_score = 0.0
                if i > 0:
                    prev_score = score_list[i-1]
                    prev_window_start = prev_score['lastUpdateTime'] - window_size_ms
                    prev_scores_in_window = [s for s in score_list if s['lastUpdateTime'] > prev_window_start]
                    previous_total_score = sum(s['score'] for s in prev_scores_in_window)
                
                results.append({
                    'userId': user_id,
                    'totalScore': total_score,
                    'previousTotalScore': previous_total_score,
                    'lastUpdateTime': current_time
                })
            
            return results
        
        # Group by user và apply function
        user_scores_rdd = scores.rdd.groupBy(lambda x: x['id']).map(calculate_user_total_scores)
        return user_scores_rdd.flatMap(lambda x: x).toDF()
    
    def calculate_leaderboard_at_snapshots(self, total_scores: DataFrame, snapshot_times: List[int], 
                                         top_n: int, ttl_minutes: int) -> List[LeaderBoardEntry]:
        """Tính leaderboard tại các snapshot times cụ thể"""
        ttl_ms = ttl_minutes * 60 * 1000
        all_snapshots = []
        
        for snapshot_time in snapshot_times:
            cutoff_time = snapshot_time - ttl_ms
            
            # Lấy tất cả scores hợp lệ tại thời điểm snapshot
            valid_scores = total_scores.filter(
                (col('lastUpdateTime') <= snapshot_time) & 
                (col('lastUpdateTime') > cutoff_time)
            ).collect()
            
            # Group by user và lấy score mới nhất cho mỗi user
            user_latest_scores = {}
            for score in valid_scores:
                user_id = score['userId']
                if user_id not in user_latest_scores or score['lastUpdateTime'] > user_latest_scores[user_id]['lastUpdateTime']:
                    user_latest_scores[user_id] = score
            
            # Sort by total score và lấy top N
            sorted_users = sorted(user_latest_scores.values(), key=lambda x: x['totalScore'], reverse=True)
            
            # Tạo leaderboard entries cho snapshot này
            for i, user_score in enumerate(sorted_users[:top_n]):
                all_snapshots.append(LeaderBoardEntry(
                    userId=user_score['userId'],
                    totalScore=user_score['totalScore'],
                    rank=i + 1,
                    lastUpdateTime=user_score['lastUpdateTime'],
                    snapshotTime=snapshot_time
                ))
        
        return all_snapshots
    
    def generate_snapshots(self, total_scores: DataFrame, top_n: int, ttl_minutes: int, 
                          snapshot_interval_minutes: int = 7) -> List[LeaderBoardEntry]:
        """Generate snapshots mỗi 7 phút theo event time"""
        # Lấy tất cả timestamps
        all_timestamps = [row['lastUpdateTime'] for row in total_scores.select('lastUpdateTime').distinct().collect()]
        all_timestamps.sort()
        
        if not all_timestamps:
            return []
        
        first_timestamp = all_timestamps[0]
        last_timestamp = all_timestamps[-1]
        
        # Generate snapshot times (mỗi 7 phút)
        snapshot_interval_ms = snapshot_interval_minutes * 60 * 1000
        snapshot_times = []
        
        # Snapshot đầu tiên sau 7 phút từ record đầu tiên
        first_snapshot_time = first_timestamp + snapshot_interval_ms
        current_time = first_snapshot_time
        
        while current_time <= last_timestamp:
            snapshot_times.append(current_time)
            current_time += snapshot_interval_ms
        
        print(f"Generated {len(snapshot_times)} snapshot times")
        for ts in snapshot_times:
            print(f"  Snapshot time: {self.format_timestamp(ts)}")
        
        # Tính leaderboard tại các snapshot times
        return self.calculate_leaderboard_at_snapshots(total_scores, snapshot_times, top_n, ttl_minutes)
    
    
    def write_snapshots(self, snapshots: List[LeaderBoardEntry], output_path: str):
        """Ghi snapshots ra file"""
        if not snapshots:
            print("No snapshots to write")
            return
        
        # Convert to DataFrame
        snapshot_data = []
        for entry in snapshots:
            snapshot_data.append({
                'userId': entry.userId,
                'totalScore': entry.totalScore,
                'rank': entry.rank,
                'lastUpdateTime': entry.lastUpdateTime,
                'snapshotTime': entry.snapshotTime,
                'snapshotTimeFormatted': self.format_timestamp(entry.snapshotTime),
                'lastUpdateTimeFormatted': self.format_timestamp(entry.lastUpdateTime)
            })
        
        snapshot_df = self.spark.createDataFrame(snapshot_data)
        
        # Write as JSON with partitioning by snapshot time
        snapshot_df.write \
            .mode("overwrite") \
            .partitionBy("snapshotTime") \
            .json(output_path)
        
        print(f"Snapshots written to: {output_path}")
        
        # Show sample results
        snapshot_df.orderBy("snapshotTime", "rank").show(50, False)
    
    def format_timestamp(self, timestamp: int) -> str:
        """Format timestamp for display"""
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        return dt.isoformat()


def main():
    parser = argparse.ArgumentParser(description='Spark LeaderBoard Processor')
    parser.add_argument('input_path', help='Path to input JSONL file')
    parser.add_argument('output_path', help='Path to output snapshots')
    parser.add_argument('--window-size', type=int, default=5, help='Window size in minutes (default: 5)')
    parser.add_argument('--top-n', type=int, default=10, help='Top N users (default: 10)')
    parser.add_argument('--ttl', type=int, default=30, help='TTL in minutes (default: 30)')
    parser.add_argument('--snapshot-interval', type=int, default=7, help='Snapshot interval in minutes (default: 7)')
    
    args = parser.parse_args()
    
    print(f"Processing leaderboard with:")
    print(f"  Input: {args.input_path}")
    print(f"  Output: {args.output_path}")
    print(f"  Window size: {args.window_size} minutes")
    print(f"  Top N: {args.top_n}")
    print(f"  TTL: {args.ttl} minutes")
    print(f"  Snapshot interval: {args.snapshot_interval} minutes")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("LeaderBoardProcessor") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        processor = LeaderBoardProcessor(spark)
        
        # Step 1: Read data (Parquet)
        print("\n=== Step 1: Reading data ===")
        user_data = processor.read_data(args.input_path)
        user_data.show(10, False)
        
        # Step 2: Transform to Score objects
        print("\n=== Step 2: Transforming to Score objects ===")
        scores = processor.transform_to_scores(user_data)
        scores.show(10, False)
        
        # Step 3: Calculate total scores in sliding window
        print("\n=== Step 3: Calculating total scores in window ===")
        total_scores = processor.calculate_total_scores_in_window(scores, args.window_size)
        total_scores.show(10, False)
        
        # Step 4: Generate snapshots
        print("\n=== Step 4: Generating snapshots ===")
        snapshots = processor.generate_snapshots(
            total_scores, 
            args.top_n, 
            args.ttl, 
            args.snapshot_interval
        )
        
        # Step 5: Write snapshots
        print("\n=== Step 5: Writing snapshots ===")
        processor.write_snapshots(snapshots, args.output_path)
        
        print(f"\nCompleted! Processed {len(snapshots)} leaderboard entries across snapshots.")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
