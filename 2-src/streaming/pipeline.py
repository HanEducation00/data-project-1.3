#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Elektrik Yük Tahmini - Kafka-Spark Streaming Pipeline

Bu modül, Kafka'dan gelen elektrik yük verilerini işleyen ana streaming pipeline'ını içerir.
"""

from utils.logger import get_logger, log_exceptions
from utils.connections import create_spark_session, read_kafka_stream
from utils.config import CHECKPOINT_PATH, STREAMING_CONFIG
from data_processing.schema import parse_kafka_json
from .batch_processor import process_batch

# Logger oluştur
logger = get_logger(__name__)

class ElectricLoadPipeline:
    """
    Elektrik yük verilerini işleyen streaming pipeline sınıfı.
    """
    
    def __init__(self):
        """Pipeline nesnesi başlatılır ve kaynaklar oluşturulur."""
        logger.info("Elektrik Yük Pipeline'ı başlatılıyor...")
        self.spark = None
        self.query = None
    
    @log_exceptions(logger)
    def initialize(self):
        """Spark Session ve gerekli kaynakları başlatır."""
        # Spark Session oluştur
        self.spark = create_spark_session()
        logger.info("Pipeline başlatıldı")
        return self
    
    @log_exceptions(logger)
    def create_stream(self):
        """Kafka stream oluşturur ve ilk ayrıştırmayı yapar."""
        if not self.spark:
            raise ValueError("Pipeline başlatılmamış. Önce initialize() metodunu çağırın.")
        
        # Kafka'dan veri akışını oku
        kafka_df = read_kafka_stream(self.spark)
        
        # JSON mesajlarını ayrıştır
        parsed_df = parse_kafka_json(kafka_df)
        
        logger.info("Kafka stream oluşturuldu ve ayrıştırıldı")
        return parsed_df
    
    @log_exceptions(logger)
    def start(self):
        """Pipeline'ı başlatır ve streaming işlemini başlatır."""
        try:
            # Stream oluştur
            stream_df = self.create_stream()
            
            # Streaming query'yi başlat
            self.query = stream_df \
                .writeStream \
                .foreachBatch(process_batch) \
                .option("checkpointLocation", CHECKPOINT_PATH) \
                .trigger(processingTime=STREAMING_CONFIG["processing_time"]) \
                .start()
            
            logger.info(f"Streaming işlemi başlatıldı. Processing time: {STREAMING_CONFIG['processing_time']}")
            return self
            
        except Exception as e:
            logger.error(f"Stream başlatılırken hata: {str(e)}")
            raise
    
    def await_termination(self):
        """Streaming işleminin tamamlanmasını bekler."""
        if self.query:
            logger.info("Stream termination bekleniyor. Durdurmak için Ctrl+C kullanın.")
            self.query.awaitTermination()
        else:
            logger.warning("Beklenecek aktif bir stream yok.")
    
    def stop(self):
        """Pipeline'ı durdurur ve kaynakları serbest bırakır."""
        try:
            if self.query:
                self.query.stop()
                logger.info("Streaming işlemi durduruldu")
            
            if self.spark:
                self.spark.stop()
                logger.info("Spark session kapatıldı")
                
        except Exception as e:
            logger.error(f"Pipeline durdurulurken hata: {str(e)}")
            raise
        finally:
            self.query = None
            self.spark = None
