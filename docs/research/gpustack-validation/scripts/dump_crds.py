#!/usr/bin/env python3
"""Dump the real Higress CRD objects from the embedded (lightweight) apiserver.

The embedded apiserver has NO namespaces (core list is empty) but serves the
CRD groups; its CRD objects are reachable via a CLUSTER-scoped list (they carry
a `namespace` in metadata, e.g. higress-system). This script does a discovery
walk over /api and /apis and lists every resource cluster-scoped (falling back
to the higress-system namespace for namespaced core resources), then emits a
multi-doc YAML stream.

Run inside the gpustack-server container:
    docker exec gpustack-server python /tmp/dump_crds.py [kubeconfig]
"""
import json
import re
import ssl
import sys
import urllib.request

import yaml


def load_api_server(kubeconfig):
    try:
        txt = open(kubeconfig).read()
        m = re.search(r"server:\s*(\S+)", txt)
        if m:
            return m.group(1)
    except Exception:
        pass
    return "https://127.0.0.1:18443"


def get(url):
    ctx = ssl._create_unverified_context()
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=20) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"__error__": f"{type(e).__name__}: {e}"}


def main(kc):
    ap = load_api_server(kc).rstrip("/")
    sys.stderr.write(f"apiserver: {ap}\n")

    items = []
    CORE_KINDS = {
        "configmaps": "ConfigMap",
        "secrets": "Secret",
        "services": "Service",
        "serviceaccounts": "ServiceAccount",
    }

    # Core v1 (namespaced resources live in higress-system here).
    for plural, kind in CORE_KINDS.items():
        for ns in ("higress-system", "default"):
            d = get(f"{ap}/api/v1/namespaces/{ns}/{plural}")
            if d.get("__error__"):
                continue
            for it in d.get("items", []):
                it["kind"] = kind
                it.setdefault("apiVersion", "v1")
                items.append(it)

    # The embedded apiserver exposes no `resources` in /apis, so hardcode the
    # Higress/Envoy CRD endpoints (verified to be cluster-listable).
    CRD_ENDPOINTS = [
        ("extensions.higress.io/v1alpha1/wasmplugins", "WasmPlugin"),
        ("networking.higress.io/v1/mcpbridges", "McpBridge"),
        ("extensions.higress.io/v1alpha1/mcpbridges", "McpBridge"),
        ("networking.istio.io/v1alpha3/envoyfilters", "EnvoyFilter"),
        ("networking.istio.io/v1/envoyfilters", "EnvoyFilter"),
        ("networking.k8s.io/v1/ingresses", "Ingress"),
        ("gateway.networking.k8s.io/v1/gateways", "Gateway"),
        ("gateway.networking.k8s.io/v1/httproutes", "HTTPRoute"),
        ("gateway.networking.k8s.io/v1/gatewayclasses", "GatewayClass"),
        ("gateway.networking.k8s.io/v1beta1/httproutes", "HTTPRoute"),
    ]
    for path, kind in CRD_ENDPOINTS:
        d = get(f"{ap}/apis/{path}")
        if d.get("__error__"):
            continue
        gv = path.rsplit("/", 1)[0]  # strip plural -> groupVersion (approx)
        for it in d.get("items", []):
            it["kind"] = kind
            it.setdefault("apiVersion", it.get("apiVersion") or gv)
            items.append(it)

    # de-dupe by (apiVersion, kind, namespace, name)
    seen = set()
    uniq = []
    for it in items:
        key = (it.get("apiVersion"), it.get("kind"),
               it.get("metadata", {}).get("namespace"), it.get("metadata", {}).get("name"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(it)

    chunks = [yaml.safe_dump(it, sort_keys=False, width=4096).rstrip() for it in uniq]
    sys.stdout.write(("\n---\n".join(chunks) + "\n") if chunks else "")
    sys.stderr.write(f"total unique objects: {len(uniq)}\n")


if __name__ == "__main__":
    kc = sys.argv[1] if len(sys.argv) > 1 else "/var/lib/gpustack/higress/kubeconfig"
    main(kc)
