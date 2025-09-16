#!/usr/bin/env python3
"""
PubSub Trace Analyzer

Analyzes go-libp2p-pubsub traces to calculate queue depth statistics.
Focuses on pubsub.process_loop_iteration spans to understand event loop performance.
"""

import json
import sys
from collections import defaultdict
from datetime import datetime

def parse_duration(duration_str):
    """Parse Go duration string to microseconds"""
    if duration_str.endswith('µs'):
        return float(duration_str[:-2])
    elif duration_str.endswith('ms'):
        return float(duration_str[:-2]) * 1000
    elif duration_str.endswith('s'):
        return float(duration_str[:-1]) * 1000000
    elif duration_str.endswith('ns'):
        return float(duration_str[:-2]) / 1000
    else:
        # Assume microseconds if no unit
        return float(duration_str)

def analyze_traces(filename):
    """Analyze PubSub traces from JSON file"""
    
    process_loop_spans = []
    
    try:
        with open(filename, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    trace = json.loads(line)
                    
                    # Only process pubsub.process_loop_iteration spans
                    if trace.get('name') == 'pubsub.process_loop_iteration':
                        attributes = trace.get('attributes', {})
                        
                        # Extract queue depths
                        sendmsg_depth = attributes.get('pubsub.sendmsg_queue_depth', 0)
                        incoming_depth = attributes.get('pubsub.incoming_queue_depth', 0)
                        iteration = attributes.get('pubsub.iteration', 0)
                        peer_count = attributes.get('pubsub.peer_count', 0)
                        topic_count = attributes.get('pubsub.topic_count', 0)
                        
                        # Parse duration
                        duration_str = trace.get('duration', '0µs')
                        duration_us = parse_duration(duration_str)
                        
                        # Parse timestamp for sorting
                        start_time = trace.get('startTime', '')
                        
                        process_loop_spans.append({
                            'iteration': iteration,
                            'sendmsg_depth': sendmsg_depth,
                            'incoming_depth': incoming_depth,
                            'duration_us': duration_us,
                            'peer_count': peer_count,
                            'topic_count': topic_count,
                            'start_time': start_time,
                            'line_num': line_num
                        })
                        
                except json.JSONDecodeError as e:
                    print(f"Warning: Invalid JSON on line {line_num}: {e}", file=sys.stderr)
                    continue
                except Exception as e:
                    print(f"Warning: Error processing line {line_num}: {e}", file=sys.stderr)
                    continue
    
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{filename}': {e}", file=sys.stderr)
        sys.exit(1)
    
    if not process_loop_spans:
        print("No pubsub.process_loop_iteration spans found in the file", file=sys.stderr)
        sys.exit(1)
    
    # Sort by start_time to get chronological order
    process_loop_spans.sort(key=lambda x: x['start_time'])
    
    # Take the last 100,000 spans
    latest_spans = process_loop_spans[-100000:] if len(process_loop_spans) > 100000 else process_loop_spans
    
    print("=" * 80)
    print("PubSub Process Loop Analysis")
    print("=" * 80)
    print(f"Total process_loop_iteration spans found: {len(process_loop_spans):,}")
    print(f"Analyzing last {len(latest_spans):,} spans")
    
    if latest_spans:
        print(f"Time range: {latest_spans[0]['start_time']} to {latest_spans[-1]['start_time']}")
        print(f"Iteration range: {latest_spans[0]['iteration']:,} to {latest_spans[-1]['iteration']:,}")
    
    print()
    
    # Calculate statistics
    sendmsg_depths = [span['sendmsg_depth'] for span in latest_spans]
    incoming_depths = [span['incoming_depth'] for span in latest_spans]
    durations = [span['duration_us'] for span in latest_spans]
    peer_counts = [span['peer_count'] for span in latest_spans]
    topic_counts = [span['topic_count'] for span in latest_spans]
    
    def calculate_stats(values, name):
        if not values:
            return
            
        total = sum(values)
        count = len(values)
        avg = total / count
        min_val = min(values)
        max_val = max(values)
        
        # Calculate percentiles
        sorted_vals = sorted(values)
        p50 = sorted_vals[int(0.5 * count)]
        p90 = sorted_vals[int(0.9 * count)]
        p95 = sorted_vals[int(0.95 * count)]
        p99 = sorted_vals[int(0.99 * count)]
        
        print(f"{name}:")
        print(f"  Average: {avg:.2f}")
        print(f"  Min: {min_val}")
        print(f"  Max: {max_val}")
        print(f"  P50 (median): {p50}")
        print(f"  P90: {p90}")
        print(f"  P95: {p95}")
        print(f"  P99: {p99}")
        
        # Count non-zero values
        non_zero = sum(1 for v in values if v > 0)
        if non_zero > 0:
            print(f"  Non-zero occurrences: {non_zero:,} ({non_zero/count*100:.1f}%)")
        print()
    
    # Print statistics
    calculate_stats(sendmsg_depths, "SendMsg Queue Depth")
    calculate_stats(incoming_depths, "Incoming Queue Depth")
    calculate_stats(durations, "Iteration Duration (µs)")
    calculate_stats(peer_counts, "Peer Count")
    calculate_stats(topic_counts, "Topic Count")
    
    # Queue depth distribution
    print("Queue Depth Distribution:")
    print("-" * 40)
    
    def print_distribution(values, name, max_show=10):
        from collections import Counter
        counter = Counter(values)
        total = len(values)
        
        print(f"{name}:")
        for depth, count in sorted(counter.items())[:max_show]:
            percentage = count / total * 100
            print(f"  Depth {depth}: {count:,} occurrences ({percentage:.1f}%)")
        
        if len(counter) > max_show:
            remaining = len(counter) - max_show
            print(f"  ... and {remaining} more depth values")
        print()
    
    print_distribution(sendmsg_depths, "SendMsg Queue")
    print_distribution(incoming_depths, "Incoming Queue")
    
    # Performance analysis
    print("Performance Analysis:")
    print("-" * 40)
    
    # Find slow iterations
    slow_iterations = [span for span in latest_spans if span['duration_us'] > 1000]  # > 1ms
    very_slow_iterations = [span for span in latest_spans if span['duration_us'] > 10000]  # > 10ms
    
    print(f"Slow iterations (>1ms): {len(slow_iterations):,} ({len(slow_iterations)/len(latest_spans)*100:.1f}%)")
    print(f"Very slow iterations (>10ms): {len(very_slow_iterations):,} ({len(very_slow_iterations)/len(latest_spans)*100:.1f}%)")
    
    # Queue backlog analysis
    sendmsg_backlog = [span for span in latest_spans if span['sendmsg_depth'] > 0]
    incoming_backlog = [span for span in latest_spans if span['incoming_depth'] > 0]
    
    print(f"Iterations with SendMsg backlog: {len(sendmsg_backlog):,} ({len(sendmsg_backlog)/len(latest_spans)*100:.1f}%)")
    print(f"Iterations with Incoming backlog: {len(incoming_backlog):,} ({len(incoming_backlog)/len(latest_spans)*100:.1f}%)")
    
    # High queue depth warnings
    high_sendmsg = [span for span in latest_spans if span['sendmsg_depth'] > 16]  # > 50% of default buffer
    high_incoming = [span for span in latest_spans if span['incoming_depth'] > 16]  # > 50% of default buffer
    
    if high_sendmsg:
        print(f"⚠️  High SendMsg queue depth (>16): {len(high_sendmsg):,} occurrences")
        max_sendmsg = max(span['sendmsg_depth'] for span in high_sendmsg)
        print(f"    Maximum SendMsg depth observed: {max_sendmsg}")
    
    if high_incoming:
        print(f"⚠️  High Incoming queue depth (>16): {len(high_incoming):,} occurrences")
        max_incoming = max(span['incoming_depth'] for span in high_incoming)
        print(f"    Maximum Incoming depth observed: {max_incoming}")
    
    # Buffer utilization
    sendmsg_utilization = (sum(sendmsg_depths) / len(sendmsg_depths)) / 32 * 100  # Assuming 32 buffer size
    incoming_utilization = (sum(incoming_depths) / len(incoming_depths)) / 32 * 100
    
    print()
    print("Buffer Utilization (assuming 32-entry buffers):")
    print(f"  SendMsg buffer: {sendmsg_utilization:.2f}% average utilization")
    print(f"  Incoming buffer: {incoming_utilization:.2f}% average utilization")
    
    if sendmsg_utilization > 25 or incoming_utilization > 25:
        print("⚠️  High buffer utilization detected - consider increasing buffer sizes")
    
    print()
    print("=" * 80)

if __name__ == "__main__":
    filename = "traces.json"
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    
    analyze_traces(filename)
