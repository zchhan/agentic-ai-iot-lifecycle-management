#!/usr/bin/env python3
"""
pi_experiment_runner_v3.py

Direct architecture:
    ESP32 Node <-> MQTT over Wi-Fi <-> Raspberry Pi 5

Formal scenarios:
    S0: normal OTA update to target firmware
    S1: corrupted OTA stream -> integrity failure -> recovery
    S2: interrupted OTA transfer -> recovery
    S3: communication/status reporting failure -> recovery

The three decision strategies share the same observation schema and action space.
Only the decision mechanism changes: rule-based, Edge LLM, or Cloud LLM.

Before formal runs:
- flash esp32_managed_node.ino to the ESP32 by USB
- run Mosquitto on the Pi
- serve firmware .bin files over HTTP
- set firmware URLs and MD5 hashes using environment variables
"""

from __future__ import annotations

import csv
import json
import os
import statistics
import time
import uuid
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

import paho.mqtt.client as mqtt
import requests

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))

STATUS_TOPIC = os.getenv("ESP32_STATUS_TOPIC", "esp32/status")
COMMAND_TOPIC = os.getenv("ESP32_COMMAND_TOPIC", "esp32/command")
ACK_TOPIC = os.getenv("ESP32_ACK_TOPIC", "esp32/ack")

GOOD_VERSION = os.getenv("GOOD_VERSION", "v1.0")
TARGET_VERSION = os.getenv("TARGET_VERSION", "v1.1")

GOOD_FW_URL = os.getenv("GOOD_FW_URL", "http://192.168.1.50:8000/firmware_v1_0.bin")
TARGET_FW_URL = os.getenv("TARGET_FW_URL", "http://192.168.1.50:8000/firmware_v1_1.bin")
GOOD_FW_MD5 = os.getenv("GOOD_FW_MD5", "")
TARGET_FW_MD5 = os.getenv("TARGET_FW_MD5", "")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:0.5b")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "")

HEARTBEAT_TIMEOUT_S = float(os.getenv("HEARTBEAT_TIMEOUT_S", "8"))
RUN_TIMEOUT_S = float(os.getenv("RUN_TIMEOUT_S", "60"))
MAX_RECOVERY_DECISIONS = int(os.getenv("MAX_RECOVERY_DECISIONS", "5"))


# ---------------------------------------------------------------------------
# Common action space
# ---------------------------------------------------------------------------

class Action(str, Enum):
    NO_ACTION = "NO_ACTION"
    WAIT = "WAIT"
    RETRY = "RETRY"
    ABORT_UPDATE = "ABORT_UPDATE"
    ROLLBACK = "ROLLBACK"
    RECONNECT = "RECONNECT"


ACTION_NAMES = [a.value for a in Action]


# ---------------------------------------------------------------------------
# Common observation schema
# ---------------------------------------------------------------------------

@dataclass
class Observation:
    current_firmware_version: str = "UNKNOWN"
    target_firmware_version: str = TARGET_VERSION
    previous_known_good_version: str = GOOD_VERSION
    heartbeat_ok: bool = False
    mqtt_connected: bool = False
    checksum_ok: bool = True
    ota_status: str = "UNKNOWN"
    retry_count: int = 0
    elapsed_s: float = 0.0
    last_event: str = ""


@dataclass
class DecisionResult:
    action: Action
    raw_output: str = ""


# ---------------------------------------------------------------------------
# Real direct-MQTT ESP32 environment
# ---------------------------------------------------------------------------

class RealESP32Env:
    def __init__(self):
        self.start_time = time.time()
        self.last_status_time = 0.0
        self.last_status: dict = {}
        self.last_ack: dict = {}
        self.retry_count = 0
        self.last_lifecycle_command: Optional[dict] = None

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        self.client.loop_start()

        # Let subscription settle.
        time.sleep(0.6)

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            client.subscribe(STATUS_TOPIC)
            client.subscribe(ACK_TOPIC)

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
        except Exception:
            return

        if msg.topic == STATUS_TOPIC:
            self.last_status = payload
            self.last_status_time = time.time()
        elif msg.topic == ACK_TOPIC:
            self.last_ack = payload

    def publish_command(self, command: dict, remember: bool = False):
        if remember:
            self.last_lifecycle_command = command.copy()
        payload = json.dumps(command, separators=(",", ":"))
        info = self.client.publish(COMMAND_TOPIC, payload, qos=1)
        info.wait_for_publish(timeout=3)

    def wait_for_first_status(self, timeout_s: float = 10.0) -> bool:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.last_status_time > 0:
                return True
            time.sleep(0.2)
        return False

    def heartbeat_ok(self) -> bool:
        if self.last_status_time == 0:
            return False
        return (time.time() - self.last_status_time) <= HEARTBEAT_TIMEOUT_S

    def get_observation(self) -> Observation:
        s = self.last_status
        return Observation(
            current_firmware_version=str(s.get("firmware_version", "UNKNOWN")),
            target_firmware_version=TARGET_VERSION,
            previous_known_good_version=GOOD_VERSION,
            heartbeat_ok=self.heartbeat_ok(),
            # Device-side MQTT state is reported in status. If reports stop, treat link as unavailable.
            mqtt_connected=bool(s.get("mqtt_connected", False)) and self.heartbeat_ok(),
            checksum_ok=bool(s.get("checksum_ok", True)),
            ota_status=str(s.get("ota_status", "UNKNOWN")),
            retry_count=self.retry_count,
            elapsed_s=time.time() - self.start_time,
            last_event=str(s.get("last_event", "")),
        )

    def initiate_scenario(self, scenario: str):
        """
        Initial lifecycle operation / fault injection.
        Fault injection is performed on the real ESP32 firmware, not by fabricating
        observations in the Pi runner.
        """
        if scenario == "S0":
            self.publish_command({
                "action": "OTA_UPDATE",
                "url": TARGET_FW_URL,
                "md5": TARGET_FW_MD5,
                "fault_mode": "none",
            }, remember=True)

        elif scenario == "S1":
            # Real integrity failure: ESP32 mutates one byte of the downloaded OTA stream
            # while the expected MD5 remains that of the valid target image.
            self.publish_command({
                "action": "OTA_UPDATE",
                "url": TARGET_FW_URL,
                "md5": TARGET_FW_MD5,
                "fault_mode": "corrupt",
            }, remember=True)

        elif scenario == "S2":
            # Real interrupted OTA: ESP32 aborts the write part-way through the stream.
            self.publish_command({
                "action": "OTA_UPDATE",
                "url": TARGET_FW_URL,
                "md5": TARGET_FW_MD5,
                "fault_mode": "interrupt",
            }, remember=True)

        elif scenario == "S3":
            # Communication/status fault: device suppresses status publishing but remains
            # subscribed to the command channel so a RECONNECT recovery command can be received.
            self.publish_command({"action": "FAULT_COMM_START"})

        else:
            raise ValueError(f"Unknown scenario: {scenario}")

    def apply_action(self, action: Action):
        if action == Action.NO_ACTION:
            return

        if action == Action.WAIT:
            time.sleep(1.0)
            return

        if action == Action.RETRY:
            self.retry_count += 1
            if self.last_lifecycle_command:
                cmd = self.last_lifecycle_command.copy()
                # A retry is a clean retry: remove the injected transfer fault.
                cmd["fault_mode"] = "none"
                self.publish_command(cmd, remember=True)
            return

        if action == Action.ROLLBACK:
            self.publish_command({
                "action": "ROLLBACK",
                "url": GOOD_FW_URL,
                "md5": GOOD_FW_MD5,
                "fault_mode": "none",
            }, remember=True)
            return

        if action == Action.RECONNECT:
            self.publish_command({"action": "RECONNECT"})
            return

        if action == Action.ABORT_UPDATE:
            self.publish_command({"action": "ABORT_UPDATE"})
            return

    def close(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Success and abnormal-state logic
# ---------------------------------------------------------------------------

TERMINAL_FAILURE_STATUSES = {
    "CHECKSUM_FAILED",
    "INTERRUPTED",
    "OTA_FAILED",
    "ABORTED",
}


def success_criteria(obs: Observation, scenario: str) -> bool:
    if scenario == "S0":
        return (
            obs.heartbeat_ok
            and obs.mqtt_connected
            and obs.current_firmware_version == TARGET_VERSION
            and obs.checksum_ok
            and obs.ota_status == "VALID"
        )

    if scenario in {"S1", "S2"}:
        # Recovery target: return to the known-good version in a valid operating state.
        return (
            obs.heartbeat_ok
            and obs.mqtt_connected
            and obs.current_firmware_version == GOOD_VERSION
            and obs.checksum_ok
            and obs.ota_status == "VALID"
        )

    if scenario == "S3":
        return obs.heartbeat_ok and obs.mqtt_connected

    return False


def abnormal_or_uncertain(obs: Observation, scenario: str) -> bool:
    if not obs.heartbeat_ok or not obs.mqtt_connected:
        return True
    if not obs.checksum_ok:
        return True
    if obs.ota_status in TERMINAL_FAILURE_STATUSES:
        return True
    if obs.ota_status == "UNKNOWN":
        return True
    return False


# ---------------------------------------------------------------------------
# Decision strategies
# ---------------------------------------------------------------------------

def build_prompt(obs: Observation, scenario: str) -> str:
    return (
        "You are an autonomous decision agent for ESP32 firmware lifecycle recovery.\n"
        "Choose exactly one next action from the allowed action space.\n\n"
        f"Scenario: {scenario}\n"
        f"Observation: {json.dumps(asdict(obs), ensure_ascii=False)}\n"
        f"Allowed actions: {', '.join(ACTION_NAMES)}\n\n"
        "Operational guidance:\n"
        "- If communication/status reporting is unavailable, choose RECONNECT.\n"
        "- If firmware integrity has failed, choose ROLLBACK.\n"
        "- If OTA was interrupted or failed, choose ROLLBACK unless a clean RETRY is clearly appropriate.\n"
        "- Do not choose destructive firmware actions for a communication-only problem.\n"
        "- Choose WAIT only when the system is still progressing and no recovery action is yet required.\n\n"
        'Return ONLY JSON in this form: {"action":"ACTION_NAME"}'
    )


def parse_action(text: str) -> Action:
    try:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            obj = json.loads(text[start:end + 1])
            return Action(str(obj["action"]).strip().upper())
    except Exception:
        pass
    return Action.WAIT


def rule_based_decide(obs: Observation, scenario: str) -> DecisionResult:
    if not obs.heartbeat_ok or not obs.mqtt_connected:
        return DecisionResult(Action.RECONNECT, "deterministic rule: communication unavailable")
    if not obs.checksum_ok:
        return DecisionResult(Action.ROLLBACK, "deterministic rule: integrity failure")
    if obs.ota_status in {"INTERRUPTED", "OTA_FAILED", "CHECKSUM_FAILED"}:
        return DecisionResult(Action.ROLLBACK, f"deterministic rule: ota_status={obs.ota_status}")
    return DecisionResult(Action.NO_ACTION, "deterministic rule: no recovery required")


def edge_llm_decide(obs: Observation, scenario: str) -> DecisionResult:
    prompt = build_prompt(obs, scenario)
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0},
            },
            timeout=45,
        )
        resp.raise_for_status()
        text = resp.json().get("response", "")
        return DecisionResult(parse_action(text), text)
    except Exception as e:
        return DecisionResult(Action.WAIT, f"EDGE_LLM_ERROR: {e}")


_anthropic_client = None


def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        if not HAS_ANTHROPIC:
            raise RuntimeError("Install anthropic first: pip install anthropic")
        if not ANTHROPIC_MODEL:
            raise RuntimeError("Set ANTHROPIC_MODEL in the environment")
        _anthropic_client = anthropic.Anthropic()
    return _anthropic_client


def cloud_llm_decide(obs: Observation, scenario: str) -> DecisionResult:
    prompt = build_prompt(obs, scenario)
    try:
        client = get_anthropic_client()
        message = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(
            block.text for block in message.content
            if getattr(block, "type", "") == "text"
        )
        return DecisionResult(parse_action(text), text)
    except Exception as e:
        return DecisionResult(Action.WAIT, f"CLOUD_LLM_ERROR: {e}")


STRATEGIES: dict[str, Callable[[Observation, str], DecisionResult]] = {
    "rule_based": rule_based_decide,
    "edge_llm": edge_llm_decide,
    "cloud_llm": cloud_llm_decide,
}


# ---------------------------------------------------------------------------
# One formal run
# ---------------------------------------------------------------------------

def run_single_trial(strategy_name: str, scenario: str) -> dict:
    run_id = f"{scenario}-{strategy_name}-{uuid.uuid4().hex[:8]}"
    env = RealESP32Env()
    strategy_fn = STRATEGIES[strategy_name]

    observations = []
    action_sequence = []
    raw_outputs = []
    decision_latencies = []

    success = False
    failure_reason = ""
    recovery_decisions = 0

    try:
        # ------------------------------------------------------------
        # 1. Receive clean initial status
        # ------------------------------------------------------------
        if not env.wait_for_first_status(timeout_s=10):
            raise RuntimeError(
                "No initial ESP32 status received. "
                "Check Wi-Fi/MQTT before running."
            )

        # Record when the scenario is actually initiated.
        scenario_sent_at = time.time()

        env.initiate_scenario(scenario)

        # Experimental timer begins with scenario initiation.
        started_at = scenario_sent_at

        # ------------------------------------------------------------
        # 2. Wait until the scenario has actually taken effect
        # ------------------------------------------------------------

        if scenario in {"S0", "S1", "S2"}:

            activation_deadline = time.time() + 10.0
            scenario_activated = False

            while time.time() < activation_deadline:
                time.sleep(0.2)

                # We need a status generated AFTER the scenario command.
                if env.last_status_time <= scenario_sent_at:
                    continue

                obs = env.get_observation()

                # OTA has genuinely started if we see a non-baseline
                # lifecycle state, an OTA event, or the target version.
                if (
                    obs.ota_status != "VALID"
                    or obs.last_event.startswith("OTA_")
                    or obs.last_event.startswith("FAULT_")
                    or obs.current_firmware_version == TARGET_VERSION
                ):
                    scenario_activated = True
                    break

            if not scenario_activated:
                failure_reason = "SCENARIO_NOT_ACTIVATED"

        elif scenario == "S3":

            # S3 suppresses status publishing, so there will deliberately
            # be no fresh status message. Wait until heartbeat timeout proves
            # that communication/status reporting has failed.
            activation_deadline = time.time() + HEARTBEAT_TIMEOUT_S + 5.0
            scenario_activated = False

            while time.time() < activation_deadline:
                obs = env.get_observation()

                if not obs.heartbeat_ok or not obs.mqtt_connected:
                    scenario_activated = True
                    break

                time.sleep(0.2)

            if not scenario_activated:
                failure_reason = "SCENARIO_NOT_ACTIVATED"

        else:
            raise ValueError(f"Unknown scenario: {scenario}")

        # Do not continue if fault/scenario injection never took effect.
        if failure_reason == "SCENARIO_NOT_ACTIVATED":
            success = False

        else:
            # ------------------------------------------------------------
            # 3. Closed-loop observation -> decision -> action
            # ------------------------------------------------------------
            while True:
                elapsed = time.time() - started_at

                if elapsed >= RUN_TIMEOUT_S:
                    failure_reason = "RUN_TIMEOUT"
                    break

                obs = env.get_observation()

                observations.append({
                    "t_s": round(elapsed, 4),
                    **asdict(obs),
                })

                # Success is checked ONLY after scenario activation.
                if success_criteria(obs, scenario):
                    success = True
                    break

                # If the system is still progressing normally,
                # keep observing without invoking the agent.
                if not abnormal_or_uncertain(obs, scenario):
                    time.sleep(0.5)
                    continue

                if recovery_decisions >= MAX_RECOVERY_DECISIONS:
                    failure_reason = "MAX_RECOVERY_DECISIONS"
                    break

                # --------------------------------------------------------
                # Agent decision
                # --------------------------------------------------------
                t0 = time.perf_counter()

                decision = strategy_fn(obs, scenario)

                latency = time.perf_counter() - t0

                decision_latencies.append(latency)
                raw_outputs.append(decision.raw_output)
                action_sequence.append(decision.action.value)
                recovery_decisions += 1

                # --------------------------------------------------------
                # Execute real recovery action
                # --------------------------------------------------------
                env.apply_action(decision.action)

                # Give physical device / MQTT / OTA time to change state.
                time.sleep(1.0)

    finally:
        env.close()

    ended_at = time.time()

    # If scenario activation failed before observations were collected,
    # record the latest state for diagnosis.
    final_obs = observations[-1] if observations else {}

    return {
        "run_id": run_id,
        "strategy": strategy_name,
        "scenario": scenario,
        "success": int(success),
        "failure_reason": failure_reason,
        "recovery_decisions": recovery_decisions,
        "action_sequence": "|".join(action_sequence),
        "decision_latency_mean_s": round(
            statistics.mean(decision_latencies), 6
        ) if decision_latencies else 0.0,
        "decision_latency_median_s": round(
            statistics.median(decision_latencies), 6
        ) if decision_latencies else 0.0,
        "decision_latencies_json": json.dumps(
            [round(x, 6) for x in decision_latencies]
        ),
        "end_to_end_latency_s": round(ended_at - started_at, 4),
        "final_state_json": json.dumps(
            final_obs,
            ensure_ascii=False
        ),
        "observations_json": json.dumps(
            observations,
            ensure_ascii=False
        ),
        "raw_outputs_json": json.dumps(
            raw_outputs,
            ensure_ascii=False
        ),
    }

def reset_device_between_runs(timeout_s: float = 30.0):
    """
    Restore a clean v1.0 baseline before each measured run.

    Reset/recovery time is outside the measured experiment.
    """

    print("[RESET] Restoring clean baseline state...")

    env = RealESP32Env()

    try:
        # First make sure status publishing is enabled after S3.
        env.publish_command({"action": "RECONNECT"})
        time.sleep(1.0)

        # Force the known-good firmware.
        env.publish_command({
            "action": "ROLLBACK",
            "url": GOOD_FW_URL,
            "md5": GOOD_FW_MD5,
            "fault_mode": "none",
        })

        # IMPORTANT:
        # Do NOT immediately trust the next VALID status.
        # The ESP32 still needs time to download, flash and reboot.
        print("[RESET] Rollback command sent; waiting for ESP32 to reboot...")
        time.sleep(6.0)

        # After the settling period, wait for a clean post-reboot state.
        deadline = time.time() + timeout_s

        consecutive_clean = 0

        while time.time() < deadline:
            time.sleep(0.5)

            obs = env.get_observation()

            clean = (
                obs.current_firmware_version == GOOD_VERSION
                and obs.heartbeat_ok
                and obs.mqtt_connected
                and obs.checksum_ok
                and obs.ota_status == "VALID"
            )

            if clean:
                consecutive_clean += 1
            else:
                consecutive_clean = 0

            # Require several consecutive clean observations.
            if consecutive_clean >= 3:
                print(
                    "[RESET] Baseline ready: "
                    f"firmware={obs.current_firmware_version}, "
                    f"mqtt={obs.mqtt_connected}, "
                    f"checksum={obs.checksum_ok}, "
                    f"ota_status={obs.ota_status}"
                )

                # Small quiet period before the measured scenario begins.
                time.sleep(1.0)
                return

        final_obs = env.get_observation()

        raise RuntimeError(
            "[RESET] Failed to restore clean baseline. "
            f"firmware={final_obs.current_firmware_version}, "
            f"heartbeat={final_obs.heartbeat_ok}, "
            f"mqtt={final_obs.mqtt_connected}, "
            f"checksum={final_obs.checksum_ok}, "
            f"ota_status={final_obs.ota_status}, "
            f"last_event={final_obs.last_event}"
        )

    finally:
        env.close()

def run_experiment(
    strategy_names: list[str],
    scenarios: list[str],
    n_repeats: int = 5,
    out_csv: str = "formal_results.csv",
):
    rows = []

    for scenario in scenarios:
        for strategy in strategy_names:
            for rep in range(1, n_repeats + 1):
                print(f"\n=== {scenario} | {strategy} | repetition {rep}/{n_repeats} ===")
                reset_device_between_runs()
                row = run_single_trial(strategy, scenario)
                row["repetition"] = rep
                rows.append(row)

                print(
                    f"success={row['success']} "
                    f"e2e={row['end_to_end_latency_s']}s "
                    f"actions={row['action_sequence']} "
                    f"failure={row['failure_reason'] or '-'}"
                )

    if not rows:
        return

    out_path = Path(out_csv)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{len(rows)} formal rows written to: {out_path.resolve()}")


if __name__ == "__main__":
    run_experiment(
        strategy_names=["rule_based"],
        scenarios=["S0", "S1", "S2", "S3"],
        n_repeats=1,
        out_csv="pilot_rule_based.csv",
    )