1- FILE WATCHER APPROACH (EN YAYGINI):
2- BATCH FİLE PROCESSOR (SCHEDULED):
3- KAFKA CONNECT FILE SOURCE (ENTERPRİSE):
4- SCADA/AMI SYSTEM INTEGRATION:
5- DIRECTORY STRUCTURE YAKLAŞIMI:


-- Optimized, temiz veri yapısı
id               - Auto increment primary key
customer_id      - Müşteri ID
profile_type     - Profil tipi  
day_num          - Gün numarası (1-365)
hour             - Saat (0-23)
minute           - Dakika (0, 15, 30, 45)
interval_idx     - Interval indexi
load_percentage  - Yük yüzdesi
full_timestamp   - Tam timestamp
created_at       - Kayıt oluşturma zamanı




-- Raw format, tüm metadata ile
raw_data         - HAM TXT SATIRI (çok uzun)
file_name        - Dosya adı
line_number      - Satır numarası
customer_id      - Müşteri ID
profile_type     - Profil tipi
year             - Yıl
month            - Ay adı
month_num        - Ay numarası
day              - Gün
date             - Tarih string
interval_id      - Interval ID
interval_idx     - Interval index
hour             - Saat
minute           - Dakika
timestamp        - Timestamp
load_percentage  - Yük yüzdesi
batch_id         - Batch ID
processing_timestamp - İşlem zamanı














-- kafka için tablo önerim:

id               - Auto increment primary key
customer_id      - Müşteri ID
profile_type     - Profil tipi  
day_num          - Gün numarası (1-365)
hour             - Saat (0-23)
minute           - Dakika (0, 15, 30, 45)
interval_idx     - Interval indexi
load_percentage  - Yük yüzdesi
full_timestamp   - Tam timestamp
created_at       - Kayıt oluşturma zamanı
timestamp        - Timestamp
load_percentage  - Yük yüzdesi
batch_id         - Batch ID
processing_timestamp - İşlem zamanı