#!/usr/bin/env python3
# Minimal OpenAI-compatible GGUF inference server for GPUStack Custom-backend
# validation. Serves a single GGUF file via llama-cpp-python (CPU build) on the
# port GPUStack assigns, exposing the model under the name GPUStack passes via
# ``--alias``. Endpoints: /health, /v1/models, /v1/chat/completions, /v1/completions.
import argparse
import json
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("-m", "--model", required=True, help="path to the GGUF file")
    p.add_argument("--port", type=int, default=8181)
    p.add_argument("--alias", default="model", help="model name reported to clients")
    p.add_argument("--n-ctx", type=int, default=8192)
    a = p.parse_args()
    return a


ARGS = parse_args()

# Lazy global; populated by load_model() before the server accepts traffic.
LLM = None
LLM_LOCK = threading.Lock()


def build_prompt(messages):
    """Qwen2.5 chat template (works for system/user/assistant turns)."""
    parts = []
    for m in messages:
        role = m.get("role", "user")
        content = m.get("content", "") or ""
        if role == "system":
            parts.append(f"<|im_start|>system\n{content}\n<|im_end|>\n")
        elif role == "assistant":
            parts.append(f"<|im_start|>assistant\n{content}\n<|im_end|>\n")
        else:
            parts.append(f"<|im_start|>user\n{content}\n<|im_end|>\n")
    parts.append("<|im_start|>assistant\n")
    return "".join(parts)


def load_model():
    global LLM
    from llama_cpp import Llama

    import os
    print(f"[serve] loading GGUF {ARGS.model} (n_ctx={ARGS.n_ctx})", flush=True)
    LLM = Llama(
        model_path=ARGS.model,
        n_ctx=ARGS.n_ctx,
        n_threads=min(8, os.cpu_count() or 8),
        verbose=False,
    )
    print(f"[serve] model loaded; alias={ARGS.alias}", flush=True)


def n_tokens(text):
    try:
        return len(LLM.tokenize(text.encode("utf-8"), add_bos=False))
    except Exception:
        return max(1, len(text) // 4)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/health", "/healthz", "/v1/health"):
            self._send(200, {"status": "ok", "model": ARGS.alias})
        elif self.path in ("/v1/models", "/models"):
            self._send(200, {"object": "list", "data": [{"id": ARGS.alias, "object": "model", "owned_by": "user"}]})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(length) or b"{}")
        except Exception as e:
            self._send(400, {"error": f"bad request: {e}"}); return

        if self.path in ("/v1/chat/completions", "/chat/completions"):
            self._chat(data)
        elif self.path in ("/v1/completions", "/completions"):
            self._completion(data)
        else:
            self._send(404, {"error": "not found"})

    def _chat(self, data):
        messages = data.get("messages", [])
        prompt = build_prompt(messages)
        max_tokens = int(data.get("max_tokens", data.get("max_completion_tokens", 256)))
        temperature = float(data.get("temperature", 0.7))
        stop = data.get("stop") or ["<|im_end|>", "<|im_start|>"]

        with LLM_LOCK:
            p_tokens = n_tokens(prompt)
            res = LLM.create_completion(
                prompt=prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                stop=stop,
                echo=False,
            )
            out = res["choices"][0]["text"]
            # Trim the trailing chat delimiters the model may have emitted.
            for marker in ("<|im_end|>", "<|im_start|>"):
                out = out.split(marker)[0]
            text = out.strip()
            c_tokens = len(res["choices"][0].get("tokens", [])) or n_tokens(text)

        self._send(200, {
            "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": ARGS.alias,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": p_tokens, "completion_tokens": c_tokens, "total_tokens": p_tokens + c_tokens},
        })

    def _completion(self, data):
        prompt = data.get("prompt", "")
        if isinstance(prompt, list):
            prompt = "".join(prompt)
        max_tokens = int(data.get("max_tokens", 256))
        temperature = float(data.get("temperature", 0.7))
        with LLM_LOCK:
            res = LLM.create_completion(prompt=prompt, max_tokens=max_tokens, temperature=temperature, echo=False)
            text = res["choices"][0]["text"]
            c_tokens = len(res["choices"][0].get("tokens", [])) or n_tokens(text)
        self._send(200, {
            "id": f"cmpl-{uuid.uuid4().hex[:24]}",
            "object": "text_completion",
            "created": int(time.time()),
            "model": ARGS.alias,
            "choices": [{"index": 0, "text": text, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": n_tokens(prompt), "completion_tokens": c_tokens, "total_tokens": n_tokens(prompt) + c_tokens},
        })


def main():
    load_model()
    srv = ThreadingHTTPServer(("0.0.0.0", ARGS.port), Handler)
    print(f"[serve] listening on 0.0.0.0:{ARGS.port} alias={ARGS.alias}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
