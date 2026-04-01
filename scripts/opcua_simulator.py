#!/usr/bin/env python3
"""OPC UA Simulator Server.

Starts a local OPC UA server with configurable simulated nodes
(temperature, pressure, humidity, vibration, discrete signals, etc.)
and dynamically updates their values so that an Edge Collector can
subscribe and receive realistic-looking telemetry.

Usage::

    python scripts/opcua_simulator.py
    python scripts/opcua_simulator.py --host 0.0.0.0 --port 4840 --interval 1.0

Node configuration lives in the ``NODES`` list below.
Add / remove / tweak entries directly in Python — no external files needed.
"""

import argparse
import asyncio
import math
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from asyncua import Server, ua

# ═══════════════════════════════════════════════════════════════════
# Enums
# ═══════════════════════════════════════════════════════════════════


class NodeType(Enum):
    """Data type of the OPC UA variable."""

    FLOAT = "float"
    INT = "int"
    BOOL = "bool"


class Pattern(Enum):
    """Value-generation strategy for numeric nodes."""

    SINE = "sine"  # smooth sine-wave oscillation + gaussian noise
    DRIFT = "drift"  # slow random-walk drift + gaussian noise
    RANDOM = "random"  # uniform random within [min, max]


# ═══════════════════════════════════════════════════════════════════
# Node definition dataclass
# ═══════════════════════════════════════════════════════════════════


@dataclass
class NodeDef:
    """Definition of a single simulated OPC UA variable.

    Parameters
    ----------
    name:
        Browse-name of the OPC UA variable (also used as the string
        node-id, e.g. ``ns=2;s=Temperature_1``).
    type:
        Data type — ``FLOAT``, ``INT``, or ``BOOL``.
    units:
        Engineering units (cosmetic, printed in the startup table).
    min, max:
        Numeric range for float/int nodes.
    noise:
        Standard deviation (σ) of additive Gaussian noise per tick.
    pattern:
        Generation strategy: ``SINE``, ``DRIFT``, or ``RANDOM``.
    sine_period:
        Full cycle duration in seconds (only for ``SINE``).
    drift_step:
        Max absolute random-walk step per tick (only for ``DRIFT``).
    toggle_probability:
        Per-tick probability of flipping a ``BOOL`` node or picking a
        new value for an ``INT`` node.
    int_values:
        Possible values for ``INT`` nodes.  If the list has duplicates
        (e.g. ``[0, 0, 0, 1, 2, 3]``) the distribution is weighted.
    """

    name: str
    type: NodeType
    units: str = ""

    # Numeric range (float / int)
    min: float = 0.0
    max: float = 100.0
    noise: float = 0.5
    pattern: Pattern = Pattern.SINE

    # Sine-specific
    sine_period: float = 60.0

    # Drift-specific
    drift_step: float = 0.3

    # Bool / discrete
    toggle_probability: float = 0.05

    # Int-specific
    int_values: list[int] = field(default_factory=list)

    # Runtime state (managed internally, not user-facing)
    _current: Any = field(default=None, init=False, repr=False)


# ═══════════════════════════════════════════════════════════════════
#  ★  NODE CONFIGURATION  —  edit this list to suit your setup  ★
# ═══════════════════════════════════════════════════════════════════

NODES: list[NodeDef] = [
    # ── Analog: temperatures ────────────────────────────────
    NodeDef(
        name="Temperature_1",
        type=NodeType.FLOAT,
        units="°C",
        min=18.0,
        max=85.0,
        noise=0.4,
        pattern=Pattern.SINE,
        sine_period=60.0,
    ),
    NodeDef(
        name="Temperature_2",
        type=NodeType.FLOAT,
        units="°C",
        min=20.0,
        max=65.0,
        noise=0.6,
        pattern=Pattern.SINE,
        sine_period=90.0,
    ),
    # ── Analog: pressure ────────────────────────────────────
    NodeDef(
        name="Pressure",
        type=NodeType.FLOAT,
        units="bar",
        min=1.0,
        max=5.5,
        noise=0.05,
        pattern=Pattern.DRIFT,
        drift_step=0.1,
    ),
    # ── Analog: humidity ────────────────────────────────────
    NodeDef(
        name="Humidity",
        type=NodeType.FLOAT,
        units="%",
        min=30.0,
        max=90.0,
        noise=0.3,
        pattern=Pattern.DRIFT,
        drift_step=0.2,
    ),
    # ── Analog: flow rate ───────────────────────────────────
    NodeDef(
        name="FlowRate",
        type=NodeType.FLOAT,
        units="m³/h",
        min=0.0,
        max=120.0,
        noise=1.0,
        pattern=Pattern.SINE,
        sine_period=120.0,
    ),
    # ── Analog: vibration ───────────────────────────────────
    NodeDef(
        name="Vibration",
        type=NodeType.FLOAT,
        units="mm/s",
        min=0.0,
        max=10.0,
        noise=0.8,
        pattern=Pattern.RANDOM,
    ),
    # ── Analog: RPM ─────────────────────────────────────────
    NodeDef(
        name="RPM",
        type=NodeType.FLOAT,
        units="rpm",
        min=800.0,
        max=3600.0,
        noise=15.0,
        pattern=Pattern.SINE,
        sine_period=45.0,
    ),
    # ── Analog: voltage ─────────────────────────────────────
    NodeDef(
        name="Voltage",
        type=NodeType.FLOAT,
        units="V",
        min=210.0,
        max=240.0,
        noise=0.3,
        pattern=Pattern.DRIFT,
        drift_step=0.5,
    ),
    # ── Discrete: motor running ─────────────────────────────
    NodeDef(
        name="MotorRunning",
        type=NodeType.BOOL,
        units="",
        toggle_probability=0.04,
    ),
    # ── Discrete: alarm code ────────────────────────────────
    NodeDef(
        name="AlarmCode",
        type=NodeType.INT,
        units="",
        int_values=[0, 0, 0, 1, 2, 3],  # weighted toward 0 (normal)
        toggle_probability=0.06,
    ),
]

# Writable setpoint/command nodes that remain unchanged unless written by a client.
STATIC_WRITABLE_NODES: list[tuple[str, NodeType, Any, str]] = [
    ("Target_Temperature", NodeType.FLOAT, 50.0, "degC"),
    ("Target_Pressure", NodeType.FLOAT, 2.5, "bar"),
    ("Pump_Enable", NodeType.BOOL, False, ""),
]


# ═══════════════════════════════════════════════════════════════════
# Value generators
# ═══════════════════════════════════════════════════════════════════


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _generate_float(node: NodeDef, t: float) -> float:
    mid = (node.min + node.max) / 2.0
    amp = (node.max - node.min) / 2.0

    if node.pattern is Pattern.SINE:
        base = mid + amp * math.sin(2.0 * math.pi * t / node.sine_period)
    elif node.pattern is Pattern.DRIFT:
        prev = node._current if node._current is not None else mid
        step = random.uniform(-node.drift_step, node.drift_step)
        base = prev + step
    else:  # RANDOM
        base = random.uniform(node.min, node.max)

    value = base + random.gauss(0.0, node.noise)
    return round(_clamp(value, node.min, node.max), 3)


def _generate_bool(node: NodeDef) -> bool:
    prev = node._current if node._current is not None else False
    if random.random() < node.toggle_probability:
        return not prev
    return prev


def _generate_int(node: NodeDef) -> int:
    prev = (
        node._current
        if node._current is not None
        else (node.int_values[0] if node.int_values else int(node.min))
    )
    if random.random() < node.toggle_probability:
        if node.int_values:
            return random.choice(node.int_values)
        return random.randint(int(node.min), int(node.max))
    return prev


def generate_value(node: NodeDef, t: float) -> Any:
    """Produce the next value for *node* at elapsed time *t* seconds."""
    if node.type is NodeType.FLOAT:
        val = _generate_float(node, t)
    elif node.type is NodeType.BOOL:
        val = _generate_bool(node)
    else:
        val = _generate_int(node)
    node._current = val
    return val


# ═══════════════════════════════════════════════════════════════════
# OPC UA helpers
# ═══════════════════════════════════════════════════════════════════

VARIANT_TYPE: dict[NodeType, ua.VariantType] = {
    NodeType.FLOAT: ua.VariantType.Double,
    NodeType.INT: ua.VariantType.Int32,
    NodeType.BOOL: ua.VariantType.Boolean,
}

INITIAL_VALUE: dict[NodeType, Any] = {
    NodeType.FLOAT: 0.0,
    NodeType.INT: 0,
    NodeType.BOOL: False,
}


# ═══════════════════════════════════════════════════════════════════
# Server
# ═══════════════════════════════════════════════════════════════════


async def run_server(host: str, port: int, interval: float) -> None:
    server = Server()
    await server.init()

    endpoint = f"opc.tcp://{host}:{port}/freeopcua/server/"
    server.set_endpoint(endpoint)
    server.set_server_name("EdgeCollector OPC UA Simulator")

    idx = await server.register_namespace("urn:edge-collector:simulator")

    # Root object for all simulated variables
    plant = await server.nodes.objects.add_object(idx, "SimulatedPlant")

    # Register every node from the NODES list
    ua_vars: dict[str, Any] = {}
    for nd in NODES:
        vt = VARIANT_TYPE[nd.type]
        node_id = ua.NodeId(nd.name, idx)  # -> ns=2;s=<Name>
        var = await plant.add_variable(
            node_id,
            nd.name,
            ua.Variant(INITIAL_VALUE[nd.type], vt),
        )
        await var.set_writable()
        ua_vars[nd.name] = var

    for name, node_type, initial_value, _units in STATIC_WRITABLE_NODES:
        vt = VARIANT_TYPE[node_type]
        node_id = ua.NodeId(name, idx)
        var = await plant.add_variable(
            node_id,
            name,
            ua.Variant(initial_value, vt),
        )
        await var.set_writable()
        ua_vars[name] = var

    # ── Print startup table ─────────────────────────────────
    separator = "=" * 72
    print()
    print(separator)
    print("  OPC UA Simulator — registered nodes")
    print(separator)
    print(f"  {'Name':<20} {'Type':<8} {'Units':<8} {'Node ID'}")
    print("-" * 72)
    for nd in NODES:
        nid = ua_vars[nd.name].nodeid.to_string()
        print(f"  {nd.name:<20} {nd.type.value:<8} {nd.units:<8} {nid}")
    for name, node_type, _initial_value, units in STATIC_WRITABLE_NODES:
        print(
            f"  {name:<20} {node_type.value:<8} {units:<8} "
            f"{ua_vars[name].nodeid.to_string()}"
        )
    print("-" * 72)
    print(f"  Endpoint : {endpoint}")
    print(f"  Namespace: {idx} (urn:edge-collector:simulator)")
    print(f"  Interval : {interval}s")
    print(f"  Nodes    : {len(ua_vars)}")
    print("  Auth     : anonymous")
    print(separator)
    print()

    # ── Start server & value-update loop ────────────────────
    async with server:
        print("[simulator] server started — press Ctrl+C to stop\n")
        t0 = time.monotonic()

        while True:
            t = time.monotonic() - t0

            for nd in NODES:
                val = generate_value(nd, t)
                vt = VARIANT_TYPE[nd.type]
                await ua_vars[nd.name].write_value(ua.Variant(val, vt))

            # Compact one-line status (first 4 nodes as a sample)
            sample = " | ".join(f"{nd.name}={nd._current}" for nd in NODES[:4])
            print(f"  [t={t:>7.1f}s] {sample}", end="\r")

            await asyncio.sleep(interval)


# ═══════════════════════════════════════════════════════════════════
# CLI entry-point
# ═══════════════════════════════════════════════════════════════════


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OPC UA simulator server with dynamically changing nodes.",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind address (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=4840,
        help="Listen port (default: 4840)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between value updates (default: 1.0)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run_server(args.host, args.port, args.interval))
    except KeyboardInterrupt:
        print("\n[simulator] stopped.")


if __name__ == "__main__":
    main()
