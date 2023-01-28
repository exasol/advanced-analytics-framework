import json
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict, List, Callable


def is_log_sequence_ok(lines: List[Dict[str, str]], line_predicate: Callable[[Dict[str, str]], bool]):
    result = False
    for line in lines:
        if line_predicate(line):
            result = True
    return result


def is_peer_is_ready_send(line: Dict[str, str]):
    return line["module"] == "peer_is_ready_sender" and line["event"] == "send"


def is_received_acknowledge_connection(line: Dict[str, str]):
    return line["module"] == "background_peer_state" and line["event"] == "received_acknowledge_connection"


def is_received_synchronize_connection(line: Dict[str, str]):
    return line["module"] == "background_peer_state" and line["event"] == "received_synchronize_connection"


def analyze_source_target_interaction():
    print("analyze_source_target_interaction")
    root = Path(__file__).parent
    with open(root / "test_add_peer.log") as f:
        group_source_target_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        collect_source_target_interaction(f, group_source_target_map)
    print_source_target_interaction(group_source_target_map)


def print_source_target_interaction(group_source_target_map):
    predicates = {
        "is_peer_is_ready_send": is_peer_is_ready_send,
        "is_received_acknowledge_connection": is_received_acknowledge_connection,
        "is_received_synchronize_connection": is_received_synchronize_connection
    }
    for group, sources in group_source_target_map.items():
        ok = Counter()
        not_ok = Counter()
        for source, targets in sources.items():
            for target, lines in targets.items():
                for predicate_name, predicate in predicates.items():
                    if not is_log_sequence_ok(lines, predicate):
                        print(f"========== {predicate_name}-{group}-{source}-{target} ============")
                        not_ok[predicate_name] += 1
                    else:
                        ok[predicate_name] += 1
        for predicate_name in predicates.keys():
            print(f"{group} {predicate_name} ok {ok[predicate_name]} not_ok {not_ok[predicate_name]}")


def collect_source_target_interaction(f, group_source_target_map):
    for line in iter(f.readline, ""):
        json_line = json.loads(line)
        if ("peer" in json_line
                and "my_connection_info" in json_line
                and "event" in json_line
                and "module" in json_line
                and json_line["event"] != "try_send"
        ):
            group = json_line["my_connection_info"]["group_identifier"]
            source = json_line["my_connection_info"]["name"]
            target = json_line["peer"]["connection_info"]["name"]
            group_source_target_map[group][source][target].append({
                "event": json_line["event"],
                "module": json_line["module"],
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
    print("analyze_close")
    root = Path(__file__).parent
    with open(root / "test_add_peer.log") as f:
        group_source_map = defaultdict(lambda: defaultdict(list))
        collect_close(f, group_source_map)
    print_close(group_source_map)


if __name__ == "__main__":
    analyze_source_target_interaction()
    analyze_close()
