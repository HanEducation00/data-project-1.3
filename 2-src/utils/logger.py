#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Loglama Yardımcı Modülü

Bu modül, uygulama genelinde tutarlı ve yapılandırılabilir loglama
işlevselliği sağlar. Farklı log seviyeleri, formatlama ve hedefler
(dosya, konsol) için merkezi bir yapılandırma sunar.
"""

import logging
import os
import sys
from datetime import datetime

# Log dizini
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
DEFAULT_LOG_LEVEL = logging.INFO

# Log dizininin varlığını kontrol et, yoksa oluştur
if not os.path.exists(LOG_DIR):
    try:
        os.makedirs(LOG_DIR)
    except Exception as e:
        print(f"Log dizini oluşturulamadı: {str(e)}")
        LOG_DIR = "."

def get_logger(name, log_level=None, log_to_file=True):
    """
    Yapılandırılmış bir logger döndürür.
    
    Args:
        name (str): Logger adı (genellikle __name__ kullanılır)
        log_level (int, optional): Log seviyesi (logging.DEBUG, logging.INFO, vb.)
        log_to_file (bool): Dosyaya log yazılsın mı?
    
    Returns:
        Logger: Yapılandırılmış logger nesnesi
    """
    if log_level is None:
        log_level = DEFAULT_LOG_LEVEL
    
    # Logger oluştur
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Halihazırda handler'ları varsa ekleme
    if logger.handlers:
        return logger
    
    # Log formatı
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Konsol handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    
    # Dosya handler (isteğe bağlı)
    if log_to_file:
        # Günlük tarih ile log dosyası oluştur
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(LOG_DIR, f"{today}_{name.replace('.', '_')}.log")
        
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(log_format)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.error(f"Log dosyası oluşturulamadı: {str(e)}")
    
    return logger

def setup_root_logger(log_level=None):
    """
    Kök logger'ı yapılandırır, böylece tüm modüller aynı yapılandırmayı kullanır.
    
    Args:
        log_level (int, optional): Log seviyesi (logging.DEBUG, logging.INFO, vb.)
    """
    if log_level is None:
        log_level = DEFAULT_LOG_LEVEL
    
    # Kök logger'ı yapılandır
    root_logger = get_logger("root", log_level)
    logging.root = root_logger

def log_exceptions(logger):
    """
    Fonksiyonları istisnaları otomatik olarak loglamak için decore eder.
    
    Args:
        logger (Logger): Kullanılacak logger nesnesi
    
    Returns:
        Function: Decorator fonksiyonu
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.exception(f"Exception in {func.__name__}: {str(e)}")
                raise
        return wrapper
    return decorator
