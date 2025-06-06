1. seasonal_schedulers.py - TETİKLEYİCİLER
Amaç: Model eğitimini başlatmak
3 ayrı DAG: Spring, Summer, Autumn tetikleyicileri
Ne yapar: Sadece log yazdırır + seasonal_ml_training'i tetikler

2. seasonal_ml_dag.py - MODEL EĞİTİMİ
Amaç: ML modellerini eğitmek
Ne yapar: Database kontrol → Veri varsa eğitim, yoksa skip

3. energy_prediction_dag.py - GÜNLÜK TAHMİN
Amaç: Eğitilmiş modelleri kullanarak günlük tahmin yapmak
Ne yapar: Model yükle → Tahmin yap → PostgreSQL'e kaydet

🔄 Çalışma Sırası:
ADIM 1: seasonal_schedulers.py çalışır
Her gün 06:00'da 3 trigger DAG çalışır
seasonal_ml_training'i tetikler

ADIM 2: seasonal_ml_dag.py çalışır
Database kontrol eder
Model eğitimi yapar (veri varsa)

ADIM 3: energy_prediction_dag.py bağımsız çalışır
Her gün 06:00'da kendi başına çalışır
Eğitilmiş modelleri kullanır