"""Offline regressions for the Responses <-> Chat tool protocol bridge.

Each group pins a defect that was reproduced in the streaming Responses path
of qoder_proxy.py:

  [1] a function tool's first argument fragment was buffered into the entry
      but never emitted as response.function_call_arguments.delta, so the
      client's concatenated arguments came up short;
  [2] response.function_call_arguments.delta/done carried no item_id;
  [3] a truncated stream (finish_reason == "length") ended with
      response.completed while the body said status == "incomplete";
  [4] custom (freeform) tools streamed the raw {"input": "..."} JSON wrapper
      as custom_tool_call_input.delta while .done carried the unwrapped
      payload, so deltas and the final item disagreed;
  [5] upstream shards that omit `index` (parallel calls all land on 0) merged
      two different calls into one;
  [6] namespace tool declarations were forwarded verbatim to an upstream chat
      endpoint that has no namespace concept.

Everything here is mocked: no network, no upstream request, no real account,
no credential fixture. Account and usage directories are redirected into a
fresh temp dir, and the desktop credential scan is switched off.

    python tests/_test_responses_protocol.py

Script-style suite (like _test_qoder.py): it prints an explicit pass/fail
count and exits 0 when nothing failed, so unittest discovery must not import
it -- hence the leading underscore.

Exit code: 0 = no failures, 1 = at least one failure.
"""
import contextlib
import io
import json
import os
import socket
import sys
import tempfile

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
sys.path.insert(0, REPO_ROOT)

_TMP = tempfile.mkdtemp(prefix="qd-responses-protocol-")
_ACCOUNTS = os.path.join(_TMP, "accounts")
_USAGE = os.path.join(_TMP, "usage")
os.environ["ACCOUNTS_DIR"] = _ACCOUNTS
os.environ["USAGE_DIR"] = _USAGE
os.environ["QD_NATIVE_IDENTITY"] = "0"
os.environ["QD_DESKTOP_DISCOVERY"] = "0"

import qoder_proxy as P  # noqa: E402
import qoder_accounts as A  # noqa: E402

PASS = 0
FAIL = 0


def check(label, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  [PASS] " + label)
    else:
        FAIL += 1
        print("  [FAIL] " + label
              + (("  " + repr(extra)) if extra != "" else ""))


# ---------------------------------------------------------------------------
# Mock upstream chat-completions stream helpers
# ---------------------------------------------------------------------------
def sse(delta, finish=None):
    """One upstream chunk, shaped exactly like iter_inner_sse yields it."""
    return ("data: " + json.dumps(
        {"choices": [{"delta": delta, "finish_reason": finish}]})
        + "\n\n").encode("utf-8")


def tool_chunk(index, call_id, name, args, has_index=True):
    """A chat tool_calls delta. has_index=False mimics shards that omit it."""
    tc = {}
    if has_index:
        tc["index"] = index
    if call_id:
        tc["id"] = call_id
        tc["type"] = "function"
    fn = {}
    if name:
        fn["name"] = name
    if args:
        fn["arguments"] = args
    tc["function"] = fn
    return sse({"tool_calls": [tc]})


def split_tool_chunks(call_id, name, blob, size=9):
    """First shard carries name/id, the rest carry raw argument fragments."""
    out = [tool_chunk(0, call_id, name, blob[:size])]
    rest = blob[size:]
    for i in range(0, len(rest), size):
        out.append(tool_chunk(0, "", "", rest[i:i + size]))
    out.append(sse({}, "tool_calls"))
    return out


def name_only_tool_chunks(call_id, name, blob, size=9):
    """First shard announces the name with empty arguments (OpenAI's shape)."""
    out = [tool_chunk(0, call_id, name, "")]
    for i in range(0, len(blob), size):
        out.append(tool_chunk(0, "", "", blob[i:i + size]))
    out.append(sse({}, "tool_calls"))
    return out


def run(chunks, model="m", holder=None):
    """Drive stream_responses_events; return [(event_name, payload), ...]."""
    if holder is None:
        holder = {"usage": None, "custom_names": set()}
    out = []
    for frame in P.stream_responses_events(iter(chunks), model, holder):
        name, payload = None, None
        for line in frame.decode("utf-8").splitlines():
            if line.startswith("event: "):
                name = line[len("event: "):]
            elif line.startswith("data: "):
                payload = json.loads(line[len("data: "):])
        out.append((name, payload))
    return out


def evs(events, name):
    return [p for n, p in events if n == name]


def item_done(events, itype):
    return [p["item"] for p in evs(events, "response.output_item.done")
            if (p.get("item") or {}).get("type") == itype]


def added_items(events):
    return {p["output_index"]: p["item"]
            for p in evs(events, "response.output_item.added")}


def joined(events, name, output_index):
    return "".join(p["delta"] for p in evs(events, name)
                   if p.get("output_index") == output_index)


ARG_DELTA = "response.function_call_arguments.delta"
ARG_DONE = "response.function_call_arguments.done"
CUSTOM_DELTA = "response.custom_tool_call_input.delta"
CUSTOM_DONE = "response.custom_tool_call_input.done"

# A patch that exercises quoting, a literal backslash escape, and non-ASCII
# text. Written with escapes so this file stays ASCII.
PATCH = ("*** Begin Patch\n"
         "*** Update File: \u4e2d\u6587.txt\n"
         "+hello \"world\"\n"
         "+tab\\there\n"
         "*** End Patch")


print("[1] function tool: first fragment carries arguments, ids stay equal")
FULL_ARGS = '{"path": "a.txt"}'
events = run([
    tool_chunk(0, "call_a", "read_file", '{"pa'),
    tool_chunk(0, "", "", 'th": "a.txt"}'),
    sse({}, "tool_calls"),
])
added = evs(events, "response.output_item.added")
deltas = evs(events, ARG_DELTA)
done = evs(events, ARG_DONE)
final = item_done(events, "function_call")
check("one function_call item is announced", len(added) == 1, added)
added_item = added[0]["item"] if added else {}
check("added item starts with empty arguments",
      added_item.get("arguments") == "", added_item)
check("every argument delta carries item_id",
      bool(deltas) and all(d.get("item_id") == added_item.get("id")
                           for d in deltas),
      [d.get("item_id") for d in deltas])
check("delta concatenation equals the full arguments",
      "".join(d["delta"] for d in deltas) == FULL_ARGS,
      "".join(d["delta"] for d in deltas))
check("done.arguments equals the full arguments",
      bool(done) and done[0]["arguments"] == FULL_ARGS, done)
check("done carries item_id",
      bool(done) and done[0].get("item_id") == added_item.get("id"), done)
check("added/done/final item ids agree",
      len(final) == 1 and final[0]["id"] == added_item.get("id")
      == done[0].get("item_id"), final)
check("added/done/final call_ids agree",
      bool(done) and len(final) == 1
      and added_item.get("call_id") == done[0].get("call_id")
      == final[0].get("call_id"),
      (added_item.get("call_id"), done[0].get("call_id") if done else None))
check("final item carries the full arguments",
      bool(final) and final[0]["arguments"] == FULL_ARGS, final)
check("terminal event is response.completed",
      events[-1][0] == "response.completed"
      and events[-1][1]["response"]["status"] == "completed", events[-1][0])

print()
print("[2] two function tools interleaved by index")
events = run([
    tool_chunk(0, "call_0", "alpha", '{"x":'),
    tool_chunk(1, "call_1", "beta", '{"y":'),
    tool_chunk(0, "", "", "1}"),
    tool_chunk(1, "", "", "2}"),
    sse({}, "tool_calls"),
])
done_by_index = {p["output_index"]: p["arguments"]
                 for p in evs(events, ARG_DONE)}
check("two distinct output items", len(done_by_index) == 2, done_by_index)
check("each tool kept its own arguments",
      sorted(done_by_index.values()) == ['{"x":1}', '{"y":2}'],
      done_by_index)
names_by_index = {i: it.get("name")
                  for i, it in added_items(events).items()}
check("names did not cross-contaminate",
      sorted(names_by_index.values()) == ["alpha", "beta"], names_by_index)
for _idx, _args in done_by_index.items():
    check("index %s deltas cover its done arguments" % _idx,
          joined(events, ARG_DELTA, _idx) == _args,
          (joined(events, ARG_DELTA, _idx), _args))

print()
print("[3] missing upstream id: the fallback id stays consistent")
events = run([tool_chunk(0, "", "ping", '{"n":1}'), sse({}, "tool_calls")])
added_item = evs(events, "response.output_item.added")[0]["item"]
done = evs(events, ARG_DONE)[0]
final = item_done(events, "function_call")[0]
call_id = added_item["call_id"]
check("a fallback call_id is generated",
      bool(call_id) and call_id.startswith("call_"), call_id)
check("added/done/final reuse the fallback call_id",
      done["call_id"] == call_id and final["call_id"] == call_id,
      (call_id, done["call_id"], final["call_id"]))
_hist = {"model": "m", "input": [
    {"type": "function_call", "name": "ping", "call_id": call_id,
     "arguments": '{"n":1}'},
    {"type": "function_call_output", "call_id": call_id, "output": "pong"},
]}
_msgs = P.responses_to_chat(_hist)["messages"]
_tool_msgs = [m for m in _msgs if m.get("role") == "tool"]
_asst = [m for m in _msgs if m.get("role") == "assistant" and m.get("tool_calls")]
check("the tool result reuses the emitted call_id",
      bool(_tool_msgs) and _tool_msgs[0]["tool_call_id"] == call_id, _tool_msgs)
check("the assistant tool_call id matches the emitted call_id",
      bool(_asst) and _asst[0]["tool_calls"][0]["id"] == call_id, _asst)

print()
print("[4] upstream shards that omit index: parallel calls stay separate")
events = run([
    tool_chunk(0, "call_a", "first", '{"a":1}', has_index=False),
    tool_chunk(0, "call_b", "second", '{"b":2}', has_index=False),
    sse({}, "tool_calls"),
])
finals = item_done(events, "function_call")
by_call = {it["call_id"]: it for it in finals}
check("both calls were emitted",
      sorted(by_call) == ["call_a", "call_b"], sorted(by_call))
check("arguments were not concatenated",
      by_call.get("call_a", {}).get("arguments") == '{"a":1}'
      and by_call.get("call_b", {}).get("arguments") == '{"b":2}', by_call)
check("names stayed with their own call",
      {it["call_id"]: it["name"] for it in finals}
      == {"call_a": "first", "call_b": "second"}, finals)

print()
print("[4b] index-less shards interleaved across two calls")
events = run([
    tool_chunk(0, "call_a", "first", '{"a":', has_index=False),
    tool_chunk(0, "call_b", "second", '{"b":', has_index=False),
    tool_chunk(0, "call_a", "", "1}", has_index=False),
    tool_chunk(0, "call_b", "", "2}", has_index=False),
    sse({}, "tool_calls"),
])
by_call = {it["call_id"]: it for it in item_done(events, "function_call")}
check("interleaved fragments went back to their own call",
      by_call.get("call_a", {}).get("arguments") == '{"a":1}'
      and by_call.get("call_b", {}).get("arguments") == '{"b":2}', by_call)

print()
print("[4c] identical tool calls are not deduplicated")
events = run([
    tool_chunk(0, "call_1", "same", '{"k":1}'),
    tool_chunk(1, "call_2", "same", '{"k":1}'),
    sse({}, "tool_calls"),
])
finals = item_done(events, "function_call")
check("both identical calls are emitted",
      len(finals) == 2
      and {it["call_id"] for it in finals} == {"call_1", "call_2"}, finals)

print()
print("[5] custom tool: deltas are the unwrapped payload, not the JSON shell")
_SHAPES = [
    ("args in the first shard", False, split_tool_chunks),
    ("name-only first shard", False, name_only_tool_chunks),
    ("\\uXXXX wrapper", True, split_tool_chunks),
]
for _mode, _ascii, _builder in _SHAPES:
    wrapper = json.dumps({"input": PATCH}, ensure_ascii=_ascii)
    holder = {"usage": None, "custom_names": {"apply_patch"}}
    events = run(_builder(0, "apply_patch", wrapper), holder=holder)
    added_item = evs(events, "response.output_item.added")[0]["item"]
    deltas = evs(events, CUSTOM_DELTA)
    done = evs(events, CUSTOM_DONE)
    final = item_done(events, "custom_tool_call")
    joined_delta = "".join(d["delta"] for d in deltas)
    check("[%s] custom item announced as custom_tool_call" % _mode,
          added_item["type"] == "custom_tool_call"
          and added_item["input"] == "", added_item)
    check("[%s] delta concatenation equals the raw patch" % _mode,
          joined_delta == PATCH, joined_delta)
    check("[%s] done.input equals the raw patch" % _mode,
          bool(done) and done[0]["input"] == PATCH,
          done[0]["input"] if done else None)
    check("[%s] final item input equals the raw patch" % _mode,
          bool(final) and final[0]["input"] == PATCH,
          final[0]["input"] if final else None)
    check("[%s] delta == done.input == final input" % _mode,
          bool(done) and bool(final)
          and joined_delta == done[0]["input"] == final[0]["input"])
    check("[%s] no JSON wrapper leaked into the deltas" % _mode,
          '"input"' not in joined_delta
          and not joined_delta.startswith('{"'), joined_delta[:80])
    check("[%s] no function_call_arguments events for a custom tool" % _mode,
          not evs(events, ARG_DELTA) and not evs(events, ARG_DONE))
    check("[%s] custom ids stay consistent" % _mode,
          bool(done) and bool(final)
          and added_item["id"] == done[0]["item_id"] == final[0]["id"]
          and added_item["call_id"] == done[0]["call_id"]
          == final[0]["call_id"])
    if _mode == "args in the first shard":
        CUSTOM_FINAL = final[0]

print()
print("[6] custom patch history round-trip, both directions")
hist = {"model": "m", "input": [
    {"role": "user", "content": "patch it"},
    {"type": "custom_tool_call", "name": "apply_patch", "call_id": "call_p",
     "input": PATCH},
    {"type": "custom_tool_call_output", "call_id": "call_p", "output": "ok"},
]}
chat = P.responses_to_chat(hist)
asst = [m for m in chat["messages"]
        if m.get("role") == "assistant" and m.get("tool_calls")]
tool_msgs = [m for m in chat["messages"] if m.get("role") == "tool"]
check("the custom call travels as the {input: ...} wrapper",
      bool(asst) and json.loads(
          asst[0]["tool_calls"][0]["function"]["arguments"])["input"] == PATCH,
      asst)
check("the custom call keeps the declared name",
      bool(asst) and asst[0]["tool_calls"][0]["function"]["name"]
      == "apply_patch", asst)
check("the custom result keeps the call_id",
      bool(tool_msgs) and tool_msgs[0]["tool_call_id"] == "call_p", tool_msgs)
hist2 = {"model": "m", "input": [
    {"role": "user", "content": "again"},
    CUSTOM_FINAL,
    {"type": "custom_tool_call_output", "call_id": CUSTOM_FINAL["call_id"],
     "output": "ok"},
]}
chat2 = P.responses_to_chat(hist2)
asst2 = [m for m in chat2["messages"]
         if m.get("role") == "assistant" and m.get("tool_calls")]
check("the emitted custom item re-enters history unwrapped",
      bool(asst2) and json.loads(
          asst2[0]["tool_calls"][0]["function"]["arguments"])["input"] == PATCH,
      asst2)

print()
print("[7] plain assistant text")
events = run([sse({"content": "Hello "}), sse({"content": "world"}),
              sse({}, "stop")])
text = "".join(p["delta"] for p in evs(events, "response.output_text.delta"))
done_txt = evs(events, "response.output_text.done")
final = item_done(events, "message")
check("text deltas concatenate", text == "Hello world", text)
check("output_text.done carries the whole text",
      bool(done_txt) and done_txt[0]["text"] == "Hello world", done_txt)
check("the message item carries the whole text",
      bool(final) and final[0]["content"][0]["text"] == "Hello world", final)
check("terminal event is response.completed",
      events[-1][0] == "response.completed", events[-1][0])

print()
print("[8] truncated stream ends with response.incomplete")
events = run([sse({"content": "partial"}), sse({}, "length")])
names = [n for n, _ in events]
terminal = events[-1]
check("no response.completed event",
      "response.completed" not in names, names)
check("terminal event is response.incomplete",
      terminal[0] == "response.incomplete", terminal[0])
check("terminal body says incomplete",
      terminal[1]["response"]["status"] == "incomplete", terminal[1]["response"])
check("incomplete_details explains the truncation",
      terminal[1]["response"].get("incomplete_details")
      == {"reason": "max_output_tokens"},
      terminal[1]["response"].get("incomplete_details"))

print()
print("[9] sequence_number ordering and restart continuation")
events = run([sse({"content": "a"}), sse({"content": "b"}),
              sse({}, "stop")])
seqs = [p["sequence_number"] for _, p in events]
check("sequence numbers start at 1 with no gaps",
      seqs == list(range(1, len(seqs) + 1)), seqs)
check("response.created is first", events[0][0] == "response.created", events[0][0])
check("the terminal event is last",
      events[-1][0] in ("response.completed", "response.incomplete",
                        "response.failed"), events[-1][0])
check("event names match their payload type",
      all(n == p["type"] for n, p in events), [n for n, _ in events])
_holder = {"usage": None, "custom_names": set()}
first = run([sse({"content": "a"}), sse({}, "stop")], holder=_holder)
second = run([sse({"content": "b"}), sse({}, "stop")], holder=_holder)
check("a restarted stream continues the sequence",
      min(p["sequence_number"] for _, p in second)
      == max(p["sequence_number"] for _, p in first) + 1,
      (max(p["sequence_number"] for _, p in first),
       min(p["sequence_number"] for _, p in second)))

print()
print("[10] namespace function tool: declare -> call -> history")
NS_TOOLS = [{
    "type": "namespace", "name": "crm", "description": "CRM tools",
    "tools": [{
        "type": "function", "name": "list_open_orders",
        "description": "List open orders",
        "parameters": {"type": "object", "properties": {}},
    }],
}]
WIRE = "crm.list_open_orders"
chat = P.responses_to_chat({"model": "m", "input": "hi", "tools": NS_TOOLS})
tools = chat.get("tools") or []
check("the namespace flattens into one chat tool", len(tools) == 1, tools)
check("no namespace object reaches the upstream",
      all(str(t.get("type") or "").lower() != "namespace" for t in tools), tools)
check("the inner tool is advertised under the flat wire name",
      bool(tools) and tools[0].get("name") == WIRE, tools)
holder = {"usage": None, "custom_names": P.custom_tool_names(NS_TOOLS)}
_, holder["tool_wire"] = P._flatten_responses_tools(NS_TOOLS)
events = run([tool_chunk(0, "call_ns", WIRE, '{"limit":5}'),
              sse({}, "tool_calls")], holder=holder)
added_item = evs(events, "response.output_item.added")[0]["item"]
done = evs(events, ARG_DONE)[0]
final = item_done(events, "function_call")[0]
check("the call comes back as the bare tool name",
      added_item["name"] == "list_open_orders", added_item)
check("the namespace travels beside the name",
      added_item.get("namespace") == "crm", added_item)
check("the flat wire name is never exposed to the client",
      added_item["name"] != WIRE and final["name"] == "list_open_orders", final)
check("the namespace survives into the final item",
      final.get("namespace") == "crm", final)
check("namespace call_ids stay consistent",
      added_item["call_id"] == done["call_id"] == final["call_id"] == "call_ns",
      (added_item["call_id"], done["call_id"], final["call_id"]))
check("namespace item ids stay consistent",
      added_item["id"] == done["item_id"] == final["id"],
      (added_item["id"], done["item_id"], final["id"]))
hchat = P.responses_to_chat({"model": "m", "tools": NS_TOOLS, "input": [
    {"type": "function_call", "name": "list_open_orders", "namespace": "crm",
     "call_id": "call_ns", "arguments": '{"limit":5}'},
    {"type": "function_call_output", "call_id": "call_ns", "output": "[]"},
]})
asst = [m for m in hchat["messages"]
        if m.get("role") == "assistant" and m.get("tool_calls")]
tool_msgs = [m for m in hchat["messages"] if m.get("role") == "tool"]
check("history restores the flat wire name for the upstream",
      bool(asst) and asst[0]["tool_calls"][0]["function"]["name"] == WIRE, asst)
check("history keeps the tool result call_id",
      bool(tool_msgs) and tool_msgs[0]["tool_call_id"] == "call_ns", tool_msgs)
sel = P.responses_to_chat({"model": "m", "input": "hi", "tools": NS_TOOLS,
                           "tool_choice": {"type": "function",
                                           "name": "list_open_orders",
                                           "namespace": "crm"}})
check("the tool_choice selector maps onto the wire name",
      sel.get("tool_choice") == {"type": "function", "name": WIRE},
      sel.get("tool_choice"))
chat_obj = {"choices": [{"finish_reason": "tool_calls", "message": {
    "role": "assistant", "content": "",
    "tool_calls": [{"id": "call_ns", "type": "function", "function": {
        "name": WIRE, "arguments": '{"limit":5}'}}]}}]}
obj = P.chat_to_response(chat_obj, "m", P.custom_tool_names(NS_TOOLS),
                         holder["tool_wire"])
item = obj["output"][0]
check("the non-streaming path restores name + namespace",
      item["name"] == "list_open_orders" and item.get("namespace") == "crm",
      item)

print()
print("[10b] the same tool name in two namespaces stays independent")
NS_TWO = [
    {"type": "namespace", "name": "crm", "tools": [
        {"type": "function", "name": "list", "description": "crm list"}]},
    {"type": "namespace", "name": "billing", "tools": [
        {"type": "function", "name": "list", "description": "billing list"}]},
]
chat = P.responses_to_chat({"model": "m", "input": "hi", "tools": NS_TWO})
tools = chat.get("tools") or []
check("both namespaced tools are advertised",
      sorted(t.get("name") for t in tools) == ["billing.list", "crm.list"],
      tools)
holder = {"usage": None, "custom_names": set()}
_, holder["tool_wire"] = P._flatten_responses_tools(NS_TWO)
events = run([tool_chunk(0, "call_b", "billing.list", '{"p":1}'),
              tool_chunk(1, "call_c", "crm.list", '{"p":2}'),
              sse({}, "tool_calls")], holder=holder)
finals = {it["call_id"]: it for it in item_done(events, "function_call")}
check("each call decodes to its own namespace",
      finals.get("call_b", {}).get("namespace") == "billing"
      and finals.get("call_c", {}).get("namespace") == "crm", finals)
check("each call decodes to the bare tool name",
      finals.get("call_b", {}).get("name") == "list"
      and finals.get("call_c", {}).get("name") == "list", finals)

print()
print("[11] namespace custom tool")
NS_CUSTOM = [{
    "type": "namespace", "name": "patchbox", "description": "editors",
    "tools": [{"type": "custom", "name": "apply_patch",
               "description": "Edit files",
               "format": {"type": "grammar", "syntax": "lark",
                          "definition": "start: /.+/"}}],
}]
WIRE_C = "patchbox.apply_patch"
custom_names = P.custom_tool_names(NS_CUSTOM)
check("a namespaced custom tool is reported under its wire name",
      custom_names == {WIRE_C}, custom_names)
chat = P.responses_to_chat({"model": "m", "input": "hi", "tools": NS_CUSTOM})
tools = chat.get("tools") or []
check("a namespaced custom tool is downgraded to a function tool",
      len(tools) == 1 and tools[0].get("type") == "function"
      and tools[0].get("name") == WIRE_C, tools)
holder = {"usage": None, "custom_names": custom_names}
_, holder["tool_wire"] = P._flatten_responses_tools(NS_CUSTOM)
events = run(split_tool_chunks(0, WIRE_C,
                               json.dumps({"input": PATCH}, ensure_ascii=False)),
             holder=holder)
final = item_done(events, "custom_tool_call")[0]
check("a namespaced custom call restores name + namespace",
      final["name"] == "apply_patch"
      and final.get("namespace") == "patchbox", final)
check("a namespaced custom call unwraps its input",
      final["input"] == PATCH, final["input"])

print()
print("[12] unsupported namespace declarations are rejected, not dropped")
BAD = [
    ("a nested namespace",
     [{"type": "namespace", "name": "a", "tools": [
         {"type": "namespace", "name": "b", "tools": []}]}]),
    ("a namespace without a name",
     [{"type": "namespace", "tools": [{"type": "function", "name": "f"}]}]),
    ("a namespace without tools",
     [{"type": "namespace", "name": "a", "tools": []}]),
    ("an unsupported inner tool type",
     [{"type": "namespace", "name": "a", "tools": [{"type": "web_search"}]}]),
    ("an inner tool without a name",
     [{"type": "namespace", "name": "a", "tools": [{"type": "function"}]}]),
]
for _label, _tools in BAD:
    try:
        P.responses_to_chat({"model": "m", "input": "hi", "tools": _tools})
        check("rejects " + _label, False, "no exception raised")
    except P.UnsupportedToolDeclaration as exc:
        check("rejects " + _label, bool(str(exc)), str(exc))
    except Exception as exc:  # pragma: no cover - wrong exception type
        check("rejects " + _label, False, repr(exc))

print()
print("[12b] top-level declarations this gateway cannot serve are refused")
TOPDECL = [
    ("an unsupported top-level tool type", [{"type": "web_search"}]),
    ("an unsupported top-level tool_search", [{"type": "tool_search"}]),
    ("a top-level declaration without a name", [{"type": "function"}]),
]
for _label, _tools in TOPDECL:
    try:
        P.responses_to_chat({"model": "m", "input": "hi", "tools": _tools})
        check("rejects " + _label, False, "no exception raised")
    except P.UnsupportedToolDeclaration as exc:
        check("rejects " + _label, bool(str(exc)), str(exc))
    except Exception as exc:  # pragma: no cover - wrong exception type
        check("rejects " + _label, False, repr(exc))
PLAIN_FN = {"type": "function", "name": "get_weather", "description": "w",
            "parameters": {"type": "object"}}
plain = P.responses_to_chat({"model": "m", "input": "hi",
                             "tools": [PLAIN_FN]})
check("a plain function tool still passes through untouched",
      plain.get("tools") == [PLAIN_FN], plain.get("tools"))

print()
print("[12c] wire-name collisions are refused, not mis-decoded")
NS_CRASH = [{"type": "namespace", "name": "crm", "tools": [
    {"type": "function", "name": "list_open_orders"}]}]
COLLIDE = [
    ("a top-level function shadowed by a namespace wire name",
     NS_CRASH + [{"type": "function", "name": "crm.list_open_orders"}]),
    ("a namespace wire name shadowed by a top-level function",
     [{"type": "function", "name": "crm.list_open_orders"}] + NS_CRASH),
    ("two namespaces flattening onto the same wire name",
     [{"type": "namespace", "name": "crm", "tools": [
         {"type": "function", "name": "list"}]},
      {"type": "namespace", "name": "crm", "tools": [
          {"type": "function", "name": "list"}]}]),
]
for _label, _tools in COLLIDE:
    try:
        P.responses_to_chat({"model": "m", "input": "hi", "tools": _tools})
        check("rejects " + _label, False, "no exception raised")
    except P.UnsupportedToolDeclaration as exc:
        check("rejects " + _label, bool(str(exc)), str(exc))
    except Exception as exc:  # pragma: no cover - wrong exception type
        check("rejects " + _label, False, repr(exc))

print()
print("[13] main(): discovery switch honoured, scheduler stopped first")
_seen = []
_orig_scan = A.scan_desktop_credentials
_orig_serve = P.ThreadingHTTPServer.serve_forever
_orig_close = P.ThreadingHTTPServer.server_close


def _fake_scan(*_a, **_k):
    _seen.append("scan")
    return []


def _fake_serve(self, *_a, **_k):
    _seen.append("serve")


def _fake_close(self, *_a, **_k):
    _seen.append("stopped_before_close" if (
        P.SCHEDULER is not None and P.SCHEDULER._stop_event.is_set())
        else "scheduler_still_running")
    return _orig_close(self)


def _free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


A.scan_desktop_credentials = _fake_scan
P.ThreadingHTTPServer.serve_forever = _fake_serve
P.ThreadingHTTPServer.server_close = _fake_close
_argv = sys.argv
try:
    for _enabled, _expect_scan in (("0", False), ("1", True)):
        os.environ["QD_DESKTOP_DISCOVERY"] = _enabled
        del _seen[:]
        sys.argv = ["qoder_proxy.py", "--port", str(_free_port()),
                    "--accounts-dir", _ACCOUNTS, "--usage-dir", _USAGE]
        with contextlib.redirect_stdout(io.StringIO()):
            P.main()
        check("QD_DESKTOP_DISCOVERY=%s -> desktop scan %s"
              % (_enabled, "ran" if _expect_scan else "skipped"),
              ("scan" in _seen) == _expect_scan, _seen)
        check("QD_DESKTOP_DISCOVERY=%s -> scheduler stopped before close"
              % _enabled,
              _seen.count("stopped_before_close") == 1, _seen)
finally:
    sys.argv = _argv
    os.environ["QD_DESKTOP_DISCOVERY"] = "0"
    A.scan_desktop_credentials = _orig_scan
    P.ThreadingHTTPServer.serve_forever = _orig_serve
    P.ThreadingHTTPServer.server_close = _orig_close

print()
print("=" * 62)
print("responses protocol: %d passed, %d failed" % (PASS, FAIL))
print("SUMMARY: TOTAL %d checks, %d passed, %d failed"
      % (PASS + FAIL, PASS, FAIL))
print("=" * 62)
sys.exit(1 if FAIL else 0)
