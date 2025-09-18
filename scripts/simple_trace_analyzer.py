#!/usr/bin/env python3
"""
Simple PubSub Trace Analyzer - Just the key metrics
"""

import json
import sys

def analyze_traces(filename):
    sendmsg_depths = []
    incoming_depths = []
    durations = []
    
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    trace = json.loads(line)
                    
                    if trace.get('name') == 'pubsub.process_loop_iteration':
                        attributes = trace.get('attributes', {})
                        sendmsg_depths.append(attributes.get('pubsub.sendmsg_queue_depth', 0))
                        incoming_depths.append(attributes.get('pubsub.incoming_queue_depth', 0))
                        
                        # Parse duration
                        duration_str = trace.get('duration', '0µs')
                        if duration_str.endswith('µs'):
                            durations.append(float(duration_str[:-2]))
                        elif duration_str.endswith('ms'):
                            durations.append(float(duration_str[:-2]) * 1000)
                        
                except (json.JSONDecodeError, ValueError):
                    continue
    
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        sys.exit(1)
    
    if not sendmsg_depths:
        print("No process_loop_iteration spans found")
        sys.exit(1)
    
    # Take last 100,000 traces
    latest_count = min(100000, len(sendmsg_depths))
    sendmsg_depths = sendmsg_depths[-latest_count:]
    incoming_depths = incoming_depths[-latest_count:]
    durations = durations[-latest_count:]
    
    # Calculate averages
    avg_sendmsg = sum(sendmsg_depths) / len(sendmsg_depths)
    avg_incoming = sum(incoming_depths) / len(incoming_depths)
    avg_duration = sum(durations) / len(durations)
    
    # Calculate max values
    max_sendmsg = max(sendmsg_depths)
    max_incoming = max(incoming_depths)
    max_duration = max(durations)
    
    # Count non-zero occurrences
    sendmsg_nonzero = sum(1 for d in sendmsg_depths if d > 0)
    incoming_nonzero = sum(1 for d in incoming_depths if d > 0)
    
    print(f"Analyzed {latest_count:,} process_loop_iteration spans (last 100,000 traces)")
    print()
    print("Queue Depth Statistics:")
    print(f"  SendMsg Queue:")
    print(f"    Average: {avg_sendmsg:.2f}")
    print(f"    Maximum: {max_sendmsg}")
    print(f"    Non-zero: {sendmsg_nonzero:,} ({sendmsg_nonzero/latest_count*100:.1f}%)")
    print()
    print(f"  Incoming Queue:")
    print(f"    Average: {avg_incoming:.2f}")
    print(f"    Maximum: {max_incoming}")
    print(f"    Non-zero: {incoming_nonzero:,} ({incoming_nonzero/latest_count*100:.1f}%)")
    print()
    print(f"  Iteration Duration:")
    print(f"    Average: {avg_duration:.2f} µs")
    print(f"    Maximum: {max_duration:.2f} µs")
    
    # Buffer utilization warnings
    if max_sendmsg > 16:
        print(f"⚠️  SendMsg buffer >50% full (max: {max_sendmsg}/32)")
    if max_incoming > 16:
        print(f"⚠️  Incoming buffer >50% full (max: {max_incoming}/32)")
    
    if avg_duration > 1000:
        print(f"⚠️  Average iteration duration >1ms ({avg_duration:.0f}µs)")

if __name__ == "__main__":
    filename = "traces.json"
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    
    analyze_traces(filename)
