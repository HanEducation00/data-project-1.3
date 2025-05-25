### 2- MEVCUT YAPI
Mevcut Kodun Çalışma Şekli (Streaming Mode):

Tek bir pipeline var: Kafka → Spark Streaming → PostgreSQL
Her şey mini-batch'lerle işleniyor:

processingTime="1 minute" ile her dakika
Gelen tüm veriler aynı pipeline'dan geçiyor
Ham veri de, işlenmiş veri de, agregasyonlar da aynı anda yazılıyor

Kafka Mesajı → Spark Streaming → Aynı anda 3 tabloya yaz:
                                  ├── raw_data
                                  ├── load_data_detail  
                                  └── monthly_average_consumption
------------------------------------------------------------------------------------------------------------------------------------------------------------
### 2- HEDEF
Lambda Architecture'da Olması Gereken:
1. Batch Layer (Ayrı bir sistem):
Günde 1 kez veya saatte 1 kez çalışan büyük batch job'lar
Tüm historical data'yı işleyen Spark batch işlemleri
HDFS veya S3'te ham veri depolama

2. Speed Layer (Ayrı bir sistem):
Gerçek zamanlı, saniyeler içinde sonuç
Redis, Cassandra gibi hızlı NoSQL
Sadece son 1-2 saatlik veri

3. Serving Layer:
Batch ve Speed sonuçlarını birleştiren
Cache mekanizmalı sorgulama katmanı

Özet Fark:
Şu an: Tek bir streaming pipeline her şeyi yapıyor
Olması gereken: 2 paralel sistem (batch + speed) + 1 serving katmanı

