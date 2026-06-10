"""
Canonical GPU contention index.

Single source of truth shared by the real collector (gpu_collector), the
synthetic generators (synthetic_collector, live_metrics fallback), so every
path produces a directly comparable 0..1 contention score.

Weighted composite (0 = idle, 1 = fully saturated):
    0.20 GPU utilization
    0.15 memory controller utilization
    0.15 memory used
    0.15 temperature (capped at 90C)
    0.15 power draw
    0.10 throttle active
    0.10 PCIe throughput (capped at 15 GB/s combined)
"""


def compute_contention_index(
    gpu_utilization: float,
    memory_controller_util: float,
    memory_used_pct: float,
    temperature_c: float,
    power_draw_pct: float,
    throttle_active: int,
    pcie_tx_mbps: float,
    pcie_rx_mbps: float,
) -> float:
    return round(
        0.20 * (gpu_utilization / 100.0)
        + 0.15 * (memory_controller_util / 100.0)
        + 0.15 * (memory_used_pct / 100.0)
        + 0.15 * min(temperature_c / 90.0, 1.0)
        + 0.15 * (power_draw_pct / 100.0)
        + 0.10 * float(throttle_active)
        + 0.10 * min((pcie_tx_mbps + pcie_rx_mbps) / 15000.0, 1.0),
        4,
    )
