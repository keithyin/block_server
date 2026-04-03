## .\nic_config_windows.ps1 -InterfaceName "Ethernet"
# Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass


param(
    [Parameter(Mandatory=$true)]
    [string]$InterfaceName
)

Write-Host "===== Starting 10G NIC Tuning for: $InterfaceName ====="

####################################
# 1. MTU（Jumbo Frame）
####################################
Write-Host "[1] Setting MTU to 9000"

# 获取接口 index
$iface = Get-NetIPInterface -InterfaceAlias $InterfaceName -AddressFamily IPv4

if ($iface -eq $null) {
    Write-Host "Interface not found!"
    exit 1
}

Set-NetIPInterface -InterfaceAlias $InterfaceName -NlMtuBytes 9000
netsh interface ipv4 show subinterfaces
$jumboName = (Get-NetAdapterAdvancedProperty -Name $InterfaceName | Where-Object { $_.DisplayName -like "*Jumbo*" }).DisplayName
if ($jumboName) {
    Set-NetAdapterAdvancedProperty -Name $InterfaceName -DisplayName $jumboName -DisplayValue "9014 Bytes" -ErrorAction SilentlyContinue
}

####################################
# 2. 开启 RSS（多核）
####################################
Write-Host "[2] Enabling RSS (Receive Side Scaling)"

Enable-NetAdapterRss -Name $InterfaceName

####################################
# 3. 设置 RSS 队列数（接近 CPU 核数）
####################################
$cpu = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors

Write-Host "[3] Setting RSS queues to $cpu"

Set-NetAdapterRss -Name $InterfaceName -MaxProcessors $cpu -ErrorAction SilentlyContinue

####################################
# 4. Offload（等价 Linux tso/gso/gro）
####################################
Write-Host "[4] Enabling Offload Features"

$features = @(
    "Large Send Offload V2 (IPv4)",
    "Large Send Offload V2 (IPv6)",
    "Receive Side Scaling",
    "TCP Checksum Offload (IPv4)",
    "TCP Checksum Offload (IPv6)",
    "UDP Checksum Offload (IPv4)",
    "UDP Checksum Offload (IPv6)"
)

foreach ($feat in $features) {
    Set-NetAdapterAdvancedProperty -Name $InterfaceName `
        -DisplayName $feat `
        -DisplayValue "Enabled" `
        -ErrorAction SilentlyContinue
}

####################################
# 5. Ring Buffer（接近 Linux ethtool -G）
####################################
Write-Host "[5] Tuning Ring Buffers"

# 注意：不同网卡名称不同
Set-NetAdapterAdvancedProperty -Name $InterfaceName `
    -DisplayName "Receive Buffers" `
    -DisplayValue "4096" `
    -ErrorAction SilentlyContinue

Set-NetAdapterAdvancedProperty -Name $InterfaceName `
    -DisplayName "Transmit Buffers" `
    -DisplayValue "4096" `
    -ErrorAction SilentlyContinue

Set-NetAdapterAdvancedProperty -Name $InterfaceName `
    -DisplayName "Interrupt Moderation" `
    -DisplayValue "Enabled"

####################################
# 6. TCP Auto-Tuning（替代 tcp_rmem）
####################################
Write-Host "[6] Enabling TCP Auto-Tuning"

netsh int tcp set global autotuninglevel=experimental

####################################
# 7. 拥塞控制（Windows 替代 BBR）
####################################
Write-Host "[7] Setting Congestion Control (cubic)"

netsh int tcp set supplemental template=internet congestionprovider=cubic

####################################
# 8. 其他 TCP 优化
####################################
Write-Host "[8] Additional TCP tuning"

netsh int tcp set global rss=enabled
# netsh int tcp set global chimney=enabled
netsh int tcp set global ecncapability=disabled
netsh int tcp set global timestamps=enabled

####################################
# Done
####################################
Write-Host "===== DONE ====="
Get-NetAdapter -Name $InterfaceName | Format-Table Name, Status, LinkSpeed