Kodda Kullanılan Tablolar ve Sütunları:
1. raw_data Tablosu
Kafka'dan gelen ham veriler için:
- raw_data (String)
- file_name (String)  
- line_number (Integer)
2. load_data_detail Tablosu
İşlenmiş detay veriler için:
- customer_id (String)
- profile_type (String)
- year (String)
- month_num (Integer)
- month_name (String)
- day (Integer)
- date (Date)
- timestamp (String) 
- hour (Integer)
- minute (Integer)
- interval_idx (Integer)
- load_percentage (Double)
3. monthly_average_consumption Tablosu
Aylık ortalamalar için:
- month_num (Integer)
- month_name (String)
- avg_load_percentage (Double)