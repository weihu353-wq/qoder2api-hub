"""A loopback HTTP client exercises the actual Responses handler twice.

Only the upstream transport and account are synthetic. The request parser,
SSE framing, protocol adapter and next-round tool history run as in service.
"""
import copy
import http.client
import io
import json
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import qoder_proxy as proxy


def upstream_bytes(deltas):
    frames = []
    for delta, finish in deltas:
        inner = {'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]}
        outer = {'statusCodeValue': 200, 'body': json.dumps(inner, ensure_ascii=False)}
        frames.append(('data: ' + json.dumps(outer, ensure_ascii=False) + '\n\n').encode('utf-8'))
    frames.append(b'data: [DONE]\n\n')
    return io.BytesIO(b''.join(frames))


def parse_events(body):
    return [json.loads(line[6:]) for line in body.splitlines()
            if line.startswith('data: ') and line[6:] != '[DONE]']


class HTTPToolRoundTripTests(unittest.TestCase):
    def run_roundtrip(self, custom=False):
        patch = '*** Begin Patch\n*** Add File: sample.txt\n+中文 \\ literal\n*** End Patch'
        arguments = json.dumps({'input': patch}) if custom else '{"command":"echo offline"}'
        name = 'apply_patch' if custom else 'exec_command'
        sent = []

        def transport(chat, **kwargs):
            sent.append(copy.deepcopy(chat))
            if len(sent) == 1:
                cut = len(arguments) // 2
                deltas = [
                    ({'tool_calls': [{'index': 0, 'id': 'call_offline_1',
                                      'function': {'name': name, 'arguments': arguments[:cut]}}]}, None),
                    ({'tool_calls': [{'index': 0,
                                      'function': {'arguments': arguments[cut:]}}]}, None),
                    ({}, 'tool_calls'),
                ]
            else:
                deltas = [({'content': 'Done'}, None), ({}, 'stop')]
            return upstream_bytes(deltas), SimpleNamespace(uid='synthetic-http-account'), None

        replacements = {
            'open_upstream': transport,
            'record_usage': mock.Mock(),
            'record_error': mock.Mock(),
            'log': mock.Mock(),
        }
        with mock.patch.multiple(proxy, **replacements), \
                mock.patch.object(proxy.Handler, '_authorized', return_value=True), \
                mock.patch.object(proxy.Handler, '_request_realm', return_value='cn'), \
                mock.patch.object(proxy.Handler, 'log_message', lambda *args: None):
            server = proxy.ThreadingHTTPServer(('127.0.0.1', 0), proxy.Handler)
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                tool = {'type': 'custom', 'name': name} if custom else {
                    'type': 'function', 'name': name,
                    'parameters': {'type': 'object', 'properties': {'command': {'type': 'string'}}},
                }
                request = {'model': 'qfmodel', 'stream': True, 'tools': [tool],
                           'input': [{'type': 'message', 'role': 'user', 'content': 'Complete the task.'}]}
                first = self.post(server.server_port, request)
                final = next(e['response'] for e in first if e['type'] == 'response.completed')
                kind = 'custom_tool_call' if custom else 'function_call'
                calls = [item for item in final['output'] if item['type'] == kind]
                self.assertEqual(len(calls), 1)
                call = calls[0]
                self.assertEqual(call['call_id'], 'call_offline_1')
                delta_kind = 'response.custom_tool_call_input.delta' if custom else 'response.function_call_arguments.delta'
                combined = ''.join(e['delta'] for e in first if e['type'] == delta_kind)
                self.assertEqual(combined, patch if custom else arguments)
                self.assertEqual(call['input'] if custom else call['arguments'], combined)
                completed_item = next(e['item'] for e in first
                                      if e['type'] == 'response.output_item.done' and e['item']['type'] == kind)
                self.assertEqual(completed_item, call)
                request['input'] += final['output'] + [{
                    'type': 'custom_tool_call_output' if custom else 'function_call_output',
                    'call_id': call['call_id'], 'output': 'applied' if custom else 'offline',
                }]
                second = self.post(server.server_port, request)
                second_final = next(e['response'] for e in second if e['type'] == 'response.completed')
                self.assertFalse(any(i['type'] in ('function_call', 'custom_tool_call')
                                     for i in second_final['output']))
                self.assertEqual(sent[1]['messages'][-1]['role'], 'tool')
                self.assertEqual(sent[1]['messages'][-1]['tool_call_id'], call['call_id'])
                self.assertEqual(sent[1]['messages'][-1]['content'], 'applied' if custom else 'offline')
                self.assertEqual(len(sent), 2)
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=5)

    def post(self, port, payload):
        client = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
        try:
            client.request('POST', '/v1/responses', body=json.dumps(payload).encode('utf-8'),
                           headers={'Content-Type': 'application/json'})
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(response.getheader('Content-Type').startswith('text/event-stream'))
            return parse_events(response.read().decode('utf-8'))
        finally:
            client.close()

    def test_function_result_survives_second_http_request(self):
        self.run_roundtrip()

    def test_patch_raw_input_survives_second_http_request(self):
        self.run_roundtrip(custom=True)


if __name__ == '__main__':
    unittest.main()
