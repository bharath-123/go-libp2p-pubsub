#!/usr/bin/env python3
"""
PubSub Queue Delay Analyzer

Analyzes pubsub.handle_incoming_rpc_detailed spans to understand queue delay statistics.
This helps identify event loop congestion and network backpressure issues.
"""

import json
import sys
import argparse
from collections import defaultdict

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

def analyze_queue_delays(filename, limit=100000):
    """Analyze queue delay statistics from RPC handling spans"""
    
    rpc_spans = []
    
    try:
        with open(filename, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    trace = json.loads(line)
                    
                    # Only process pubsub.handle_incoming_rpc_detailed spans
                    if trace.get('name') == 'pubsub.handle_incoming_rpc_detailed':
                        attributes = trace.get('attributes', {})
                        
                        # Extract queue delay
                        queue_delay_ms = attributes.get('pubsub.queue_delay_ms')
                        network_to_processing_ms = attributes.get('pubsub.network_to_processing_ms')
                        
                        # Skip if no queue delay data
                        if queue_delay_ms is None:
                            continue
                            
                        # Extract other relevant attributes
                        peer_id = attributes.get('pubsub.peer_id', 'unknown')
                        message_count = attributes.get('pubsub.message_count', 0)
                        control_message_count = attributes.get('pubsub.control_message_count', 0)
                        total_duration_ms = attributes.get('pubsub.total_duration_ms', 0)
                        accept_status = attributes.get('pubsub.accept_status', 'unknown')
                        
                        # Parse span duration
                        duration_str = trace.get('duration', '0µs')
                        duration_us = parse_duration(duration_str)
                        
                        # Parse timestamp
                        start_time = trace.get('startTime', '')
                        
                        rpc_spans.append({
                            'queue_delay_ms': queue_delay_ms,
                            'network_to_processing_ms': network_to_processing_ms,
                            'total_duration_ms': total_duration_ms,
                            'duration_us': duration_us,
                            'peer_id': peer_id,
                            'message_count': message_count,
                            'control_message_count': control_message_count,
                            'accept_status': accept_status,
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
    
    if not rpc_spans:
        print("No pubsub.handle_incoming_rpc_detailed spans found in the file", file=sys.stderr)
        sys.exit(1)
    
    # Sort by start_time to get chronological order
    rpc_spans.sort(key=lambda x: x['start_time'])
    
    # Take the last N spans if limit is specified
    if limit and len(rpc_spans) > limit:
        latest_spans = rpc_spans[-limit:]
        print(f"Analyzing last {limit:,} of {len(rpc_spans):,} RPC spans")
    else:
        latest_spans = rpc_spans
        print(f"Analyzing all {len(latest_spans):,} RPC spans")
    
    if latest_spans:
        print(f"Time range: {latest_spans[0]['start_time']} to {latest_spans[-1]['start_time']}")
    
    print()
    
    # Extract queue delays for analysis
    queue_delays = [span['queue_delay_ms'] for span in latest_spans if span['queue_delay_ms'] is not None]
    
    if not queue_delays:
        print("No queue delay data found in spans")
        sys.exit(1)
    
    def calculate_stats(values, name, unit="ms"):
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
        print(f"  Count: {count:,}")
        print(f"  Average: {avg:.2f} {unit}")
        print(f"  Min: {min_val} {unit}")
        print(f"  Max: {max_val} {unit}")
        print(f"  P50 (median): {p50} {unit}")
        print(f"  P90: {p90} {unit}")
        print(f"  P95: {p95} {unit}")
        print(f"  P99: {p99} {unit}")
        print()
    
    # Main statistics
    print("=" * 60)
    print("QUEUE DELAY STATISTICS")
    print("=" * 60)
    calculate_stats(queue_delays, "Queue Delay (Network → Event Loop)")
    
    # Delay categories
    print("Queue Delay Distribution:")
    print("-" * 40)
    
    zero_delay = sum(1 for d in queue_delays if d == 0)
    low_delay = sum(1 for d in queue_delays if 0 < d <= 5)
    medium_delay = sum(1 for d in queue_delays if 5 < d <= 20)
    high_delay = sum(1 for d in queue_delays if 20 < d <= 50)
    very_high_delay = sum(1 for d in queue_delays if d > 50)
    
    total = len(queue_delays)
    
    print(f"  No delay (0ms): {zero_delay:,} ({zero_delay/total*100:.1f}%)")
    print(f"  Low delay (1-5ms): {low_delay:,} ({low_delay/total*100:.1f}%)")
    print(f"  Medium delay (6-20ms): {medium_delay:,} ({medium_delay/total*100:.1f}%)")
    print(f"  High delay (21-50ms): {high_delay:,} ({high_delay/total*100:.1f}%)")
    print(f"  Very high delay (>50ms): {very_high_delay:,} ({very_high_delay/total*100:.1f}%)")
    print()
    
    # Performance warnings
    print("Performance Analysis:")
    print("-" * 40)
    
    concerning_delays = sum(1 for d in queue_delays if d > 10)
    critical_delays = sum(1 for d in queue_delays if d > 50)
    
    print(f"Concerning delays (>10ms): {concerning_delays:,} ({concerning_delays/total*100:.1f}%)")
    print(f"Critical delays (>50ms): {critical_delays:,} ({critical_delays/total*100:.1f}%)")
    
    if concerning_delays > total * 0.05:  # More than 5%
        print("⚠️  High percentage of concerning delays - event loop may be overloaded")
    
    if critical_delays > 0:
        print(f"🚨 Critical delays detected - risk of network timeouts and peer disconnections")
        
        # Find worst delays
        worst_delays = sorted([span for span in latest_spans if span['queue_delay_ms'] > 50], 
                             key=lambda x: x['queue_delay_ms'], reverse=True)[:5]
        
        print("\nWorst Queue Delays:")
        for i, span in enumerate(worst_delays, 1):
            print(f"  {i}. {span['queue_delay_ms']}ms delay from peer {span['peer_id'][:16]}...")
            print(f"     Messages: {span['message_count']}, Controls: {span['control_message_count']}")
            print(f"     Total RPC duration: {span['total_duration_ms']}ms")
    
    # Correlation analysis
    print()
    print("Correlation Analysis:")
    print("-" * 40)
    
    # Group by message count
    by_message_count = defaultdict(list)
    for span in latest_spans:
        msg_bucket = min(span['message_count'] // 5 * 5, 20)  # Bucket by 5s, max 20+
        by_message_count[msg_bucket].append(span['queue_delay_ms'])
    
    print("Average queue delay by message count:")
    for msg_count in sorted(by_message_count.keys()):
        delays = by_message_count[msg_count]
        avg_delay = sum(delays) / len(delays)
        count = len(delays)
        if msg_count == 20:
            print(f"  20+ messages: {avg_delay:.1f}ms avg ({count:,} RPCs)")
        else:
            print(f"  {msg_count}-{msg_count+4} messages: {avg_delay:.1f}ms avg ({count:,} RPCs)")
    
    # Group by control message count
    by_control_count = defaultdict(list)
    for span in latest_spans:
        ctrl_bucket = min(span['control_message_count'] // 10 * 10, 50)  # Bucket by 10s
        by_control_count[ctrl_bucket].append(span['queue_delay_ms'])
    
    print("\nAverage queue delay by control message count:")
    for ctrl_count in sorted(by_control_count.keys()):
        delays = by_control_count[ctrl_count]
        avg_delay = sum(delays) / len(delays)
        count = len(delays)
        if ctrl_count == 50:
            print(f"  50+ controls: {avg_delay:.1f}ms avg ({count:,} RPCs)")
        else:
            print(f"  {ctrl_count}-{ctrl_count+9} controls: {avg_delay:.1f}ms avg ({count:,} RPCs)")
    
    # Buffer risk analysis
    print()
    print("Buffer Risk Analysis:")
    print("-" * 40)
    
    # Assuming 32-entry incoming buffer and typical RPC rate
    buffer_risk_delays = sum(1 for d in queue_delays if d > 32)  # 32ms at 1000 RPC/s fills buffer
    
    print(f"Buffer overflow risk (>32ms): {buffer_risk_delays:,} ({buffer_risk_delays/total*100:.1f}%)")
    
    if buffer_risk_delays > 0:
        print("🚨 Some delays exceed buffer capacity - risk of network hangs")
        print("   Consider increasing incoming buffer size from 32 to 128+ entries")
    
    # Recommendations
    print()
    print("Recommendations:")
    print("-" * 40)
    
    avg_delay = sum(queue_delays) / len(queue_delays)
    max_delay = max(queue_delays)
    
    if avg_delay < 1:
        print("✅ Excellent: Very low average queue delay")
    elif avg_delay < 5:
        print("✅ Good: Low average queue delay")
    elif avg_delay < 20:
        print("⚠️  Moderate: Consider optimizing event loop processing")
    else:
        print("🚨 Poor: High average queue delay - immediate optimization needed")
    
    if max_delay > 100:
        print("🚨 Critical: Maximum delays >100ms risk network partitioning")
        print("   - Increase incoming buffer size")
        print("   - Optimize validation pipeline")
        print("   - Consider parallel processing")
    elif max_delay > 50:
        print("⚠️  Warning: Maximum delays >50ms risk connection timeouts")
        print("   - Monitor for patterns")
        print("   - Optimize slow event loop operations")
    
    print()
    print("=" * 60)

def main():
    parser = argparse.ArgumentParser(
        description='Analyze PubSub queue delay statistics from trace files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 queue_delay_analyzer.py traces.json
  python3 queue_delay_analyzer.py traces.json --limit 50000
  python3 queue_delay_analyzer.py /path/to/traces.json --no-limit
        """
    )
    
    parser.add_argument('filename', help='Path to the trace JSON file')
    parser.add_argument('--limit', type=int, default=100000,
                       help='Limit analysis to last N spans (default: 100000)')
    parser.add_argument('--no-limit', action='store_true',
                       help='Analyze all spans (overrides --limit)')
    
    args = parser.parse_args()
    
    limit = None if args.no_limit else args.limit
    
    analyze_queue_delays(args.filename, limit)

if __name__ == "__main__":
    main()
