#### 1- Docker genel ve spark kullanımı
#### Aktif container'ları gör
docker ps
#### Real-time container resource monitoring
docker stats
#### Specific container stats (eğer container name biliyorsan)
docker stats spark-client

#### 2- Container İçi Spark Kullanımı
# Container'a bağlan
docker exec -it spark-client bash

# Container içinde tools kur
apt update
apt install htop
apt install openjdk-11-jdk-headless
apt install sysstat

"""
🖥️ htop: Görsel process monitor (CPU, RAM, process tree)
☕ openjdk-11-jdk: Java tools (jstat, jps, jmap) for JVM monitoring
📈 sysstat: System performance monitoring (iostat, vmstat)
"""

windows shell
Get-Process -Name vmmem | Select-Object Name, Id, @{Name="RAM_MB";Expression={[math]::Round($_.WorkingSet / 1MB, 2)}} | Format-Table -AutoSize

aktif kullnım
docker stats --no-stream