#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Local Raw to DB Pipeline - Ana Çalıştırma Dosyası

Bu pipeline local elektrik yük dosyalarını okur ve PostgreSQL'e kaydeder.
"""

import sys
import os
from datetime import datetime

# Pipeline modüllerini import et
from .utils.config import DATA_CONFIG, POSTGRES_CONFIG, TABLE_NAME
from .utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from .utils.connections import cleanup_connections
from .data_processing.processor import run_pipeline

# Ana logger
logger = get_logger('MAIN')

def main():
    """Ana pipeline fonksiyonu"""
    
    # Pipeline başlangıcını logla
    log_pipeline_start()
    
    try:
        # Konfigürasyon bilgilerini göster
        logger.log_config_info(DATA_CONFIG, "Veri Konfigürasyonu")
        logger.log_config_info(POSTGRES_CONFIG, "PostgreSQL Konfigürasyonu")
        logger.info(f"Hedef tablo: {TABLE_NAME}")
        
        # Pipeline'ı çalıştır
        logger.info("Pipeline başlatılıyor...")
        success = run_pipeline()
        
        if success:
            logger.info("Pipeline başarıyla tamamlandı!")
            return 0
        else:
            logger.error("Pipeline başarısız!")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("Pipeline kullanıcı tarafından durduruldu (Ctrl+C)")
        return 1
        
    except Exception as e:
        logger.log_error_with_details(e, "MAIN")
        return 1
        
    finally:
        # Temizlik işlemleri
        cleanup_connections()
        log_pipeline_end()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
