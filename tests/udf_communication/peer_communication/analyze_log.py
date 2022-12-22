import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List


def is_log_sequence_complete(lines: List[Dict[str, str]]):
    is_peer_is_ready_send_found = False
    is_received_acknowledge_connection_found = False
    is_received_synchronize_connection_found = False
    for line in lines:
        if is_peer_is_ready_send(line):
            is_peer_is_ready_send_found = True
        if is_received_acknowledge_connection(line):
            is_received_acknowledge_connection_found = True
        if is_received_synchronize_connection(line):
            is_received_synchronize_connection_found = True
    return is_peer_is_ready_send_found \
           and is_received_acknowledge_connection_found \
           and is_received_synchronize_connection_found


def is_peer_is_ready_send(line: Dict[str, str]):
    return line["clazz"] == "PeerIsReadySender" and line["event"] == "send"


def is_received_acknowledge_connection(line: Dict[str, str]):
    return line["clazz"] == "BackgroundPeerState" and line["event"] == "received_acknowledge_connection"


def is_received_synchronize_connection(line: Dict[str, str]):
    return line["clazz"] == "BackgroundPeerState" and line["event"] == "received_synchronize_connection"


def analyze_source_target_interaction():
    print("analyze_source_target_interaction")
    root = Path(__file__).parent
    with open(root / "test_add_peer.log") as f:
        group_source_target_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        collect_source_target_interaction(f, group_source_target_map)
    print_source_target_interaction(group_source_target_map)


def print_source_target_interaction(group_source_target_map):
    for group, sources in group_source_target_map.items():
        ok = 0
        not_ok = 0
        for source, targets in sources.items():
            for target, lines in targets.items():
                if not is_log_sequence_complete(lines):
                    print(f"========== {group}-{source}-{target} ============")
                    for line in lines:
                        print(group, source, target, line)
                    print()
                    not_ok += 1
                else:
                    ok += 1
        print(f"{group} ok {ok} not_ok {not_ok}")


def collect_source_target_interaction(f, group_source_target_map):
    for line in iter(f.readline, ""):
        json_line = json.loads(line)
        if ("peer" in json_line
                and "my_connection_info" in json_line
                and "event" in json_line
                and "clazz" in json_line
        ):
            group = json_line["my_connection_info"]["group_identifier"]
            source = json_line["my_connection_info"]["name"]
            target = json_line["peer"]["connection_info"]["name"]
            group_source_target_map[group][source][target].append({
                "event": json_line["event"],
                "clazz": json_line["clazz"],
                "timestamp": json_line["timestamp"],
            })


def collect_close(f, group_source_map):
    for line in iter(f.readline, ""):
        json_line = json.loads(line)
        if ("name" in json_line
                and "group_identifier" in json_line
                and "event" in json_line
                and json_line["event"].startswith("after")
        ):
            group = json_line["group_identifier"]
            source = json_line["name"]
            group_source_map[group][source].append({
                "timestamp": json_line["timestamp"],
                "event": json_line["event"]
            })


def print_close(group_source_map):
    for group, sources in group_source_map.items():
        for source, lines in sources.items():
            if len(lines) != 2:
                print(f"============== {group}-{source} ===============")
                for line in lines:
                    print(line)
        print(f"{group} after ... {len(sources)}")


def analyze_close():
    print("analyze_source_target_interaction")
    root = Path(__file__).parent
    with open(root / "test_add_peer.log") as f:
        group_source_map = defaultdict(lambda: defaultdict(list))
        collect_close(f, group_source_map)
    print_close(group_source_map)


if __name__ == "__main__":
    analyze_source_target_interaction()
    analyze_close()
