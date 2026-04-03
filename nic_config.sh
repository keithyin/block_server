#!/usr/bin/env bash

# =================================================================
# 10G NIC Tuning Script (Production Version - Throughput Optimized)
# 特点：
# - 强制 Jumbo Frame
# - 吞吐优先 + 控制风险
# - 支持大核数
# - 可参数控制
# - sudo /bin/bash nic_config.sh enp195s0f1
# =================================================================

set -e

IFACE=$1

if [ -z "$IFACE" ]; then
  echo "Usage: $0 <network_interface>"
  exit 1
fi

if ! ip link show "$IFACE" > /dev/null 2>&1; then
  echo "Error: Interface $IFACE not found."
  exit 1
fi

echo "===== Starting 10G Tuning for: $IFACE ====="

####################################
# 参数（可调）
####################################
RING_FACTOR=${RING_FACTOR:-0.8}     # ring buffer 使用比例（避免拉满）
ENABLE_IRQBALANCE=${ENABLE_IRQBALANCE:-1}

####################################
# 1. Ring Buffer（避免拉满）
####################################
echo "[1] Optimizing Ring Buffers"

RX_MAX=$(ethtool -g "$IFACE" 2>/dev/null | awk '/Pre-set maximums:/ {flag=1} flag && /RX:/ {print $2; exit}')
TX_MAX=$(ethtool -g "$IFACE" 2>/dev/null | awk '/Pre-set maximums:/ {flag=1} flag && /TX:/ {print $2; exit}')

calc_target() {
  local max=$1
  local factor=$2
  awk -v m="$max" -v f="$factor" 'BEGIN {printf "%d", m * f}'
}

if [[ "$RX_MAX" =~ ^[0-9]+$ ]]; then
  RX_TARGET=$(calc_target "$RX_MAX" "$RING_FACTOR")
  echo "Setting RX ring buffer to $RX_TARGET / $RX_MAX"
  ethtool -G "$IFACE" rx "$RX_TARGET" || true
fi

if [[ "$TX_MAX" =~ ^[0-9]+$ ]]; then
  TX_TARGET=$(calc_target "$TX_MAX" "$RING_FACTOR")
  echo "Setting TX ring buffer to $TX_TARGET / $TX_MAX"
  ethtool -G "$IFACE" tx "$TX_TARGET" || true
fi

####################################
# 2. MTU（强制开启）
####################################
echo "[2] Setting MTU 9000 (Jumbo Frame REQUIRED)"
ip link set dev "$IFACE" mtu 9000

####################################
# 3. Offload + 中断优化
####################################
echo "[3] Enabling NIC Offload"

for feat in tso gso gro lro; do
  ethtool -K "$IFACE" "$feat" on 2>/dev/null || true
done

# 自适应中断
ethtool -C "$IFACE" adaptive-rx on adaptive-tx on 2>/dev/null || true

####################################
# 4. 多队列（硬件优先）
####################################
echo "[4] Configuring Queues"

CPU_CORES=$(nproc)
MAX_Q=$(ethtool -l "$IFACE" 2>/dev/null | awk '/Combined:/ {print $2}' | head -n1)

if [[ "$MAX_Q" =~ ^[0-9]+$ ]] && [ "$MAX_Q" -gt 0 ]; then
  TARGET_Q=$(( CPU_CORES < MAX_Q ? CPU_CORES : MAX_Q ))
  echo "Setting combined queues to $TARGET_Q"
  ethtool -L "$IFACE" combined "$TARGET_Q" || true
fi

####################################
# 5. RPS/XPS（只在必要时启用）
####################################
echo "[5] Configuring RPS/XPS"

# 生成 CPU mask（支持 >32 核）
CPU_MASK=$(printf "%x" $(( (1 << (CPU_CORES > 32 ? 32 : CPU_CORES)) - 1 )))

if [[ "$TARGET_Q" -ge "$CPU_CORES" ]]; then
  echo "Hardware queues sufficient → disable RPS/XPS"
  for q in /sys/class/net/"$IFACE"/queues/rx-*; do echo 0 > "$q/rps_cpus"; done
else
  echo "Enabling RPS/XPS"
  for RXQ in /sys/class/net/"$IFACE"/queues/rx-*; do echo "$CPU_MASK" > "$RXQ/rps_cpus" 2>/dev/null || true; done
  for TXQ in /sys/class/net/"$IFACE"/queues/tx-*; do echo "$CPU_MASK" > "$TXQ/xps_cpus" 2>/dev/null || true; done
fi

####################################
# 6. TCP 栈调优
####################################
echo "[6] Applying sysctl tuning"

sysctl -w net.core.rmem_max=268435456
sysctl -w net.core.wmem_max=268435456

sysctl -w net.ipv4.tcp_rmem="4096 87380 268435456"
sysctl -w net.ipv4.tcp_wmem="4096 65536 268435456"

sysctl -w net.core.netdev_max_backlog=350000
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535

sysctl -w net.ipv4.tcp_window_scaling=1
sysctl -w net.ipv4.tcp_timestamps=1
sysctl -w net.ipv4.tcp_sack=1

####################################
# 7. BBR（推荐）
####################################
echo "[7] Enabling BBR"

modprobe tcp_bbr 2>/dev/null || true
sysctl -w net.core.default_qdisc=fq
sysctl -w net.ipv4.tcp_congestion_control=bbr

####################################
# 8. irqbalance（可控）
####################################
echo "[8] irqbalance"

if [ "$ENABLE_IRQBALANCE" -eq 1 ]; then
  systemctl start irqbalance 2>/dev/null || true
  echo "irqbalance enabled"
else
  echo "irqbalance disabled (manual tuning expected)"
fi

####################################
# Done
####################################
echo "===== DONE ====="
echo "Interface: $IFACE"
echo "MTU: $(ip link show $IFACE | grep mtu)"
echo "Congestion Control: $(sysctl -n net.ipv4.tcp_congestion_control)"