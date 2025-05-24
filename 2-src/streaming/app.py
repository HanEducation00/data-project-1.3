#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Elektrik Yük Tahmini - Ana Uygulama

Bu script, Kafka'dan gelen elektrik yük verilerini alır, işler,
aylık ortalama elektrik tüketimini hesaplar ve sonuçları PostgreSQL'e yazar.
"""

import sys
import signal
import time
from utils.logger import get_logger, setup_root_logger
from .pipeline import ElectricLoadPipeline

# Kök logger'ı yapılandır
setup_root_logger()

# Bu modül için logger oluştur
logger = get_logger(__name__)

# Global pipeline nesnesi
pipeline = None

def signal_handler(sig, frame):
    """Ctrl+C sinyalini yakalar ve düzgün bir şekilde kapatır."""
    logger.info("Kapatma sinyali alındı. Uygulama kapatılıyor...")
    if pipeline:
        pipeline.stop()
    sys.exit(0)

def main():
    """Ana uygulama fonksiyonu."""
    global pipeline
    
    try:
        logger.info("Elektrik Yük Tahmini uygulaması başlatılıyor...")
        
        # Sinyal işleyicisini kaydet
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Pipeline'ı başlat
        pipeline = ElectricLoadPipeline()
        pipeline.initialize().start()
        
        # Streaming işleminin tamamlanmasını bekle
        pipeline.await_termination()
        
    except Exception as e:
        logger.error(f"Uygulama çalışırken beklenmeyen hata: {str(e)}")
        if pipeline:
            pipeline.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
