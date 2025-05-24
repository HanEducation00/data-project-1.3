"""
Elektrik Yük Tahmini - Streaming Paketi

Bu paket, elektrik yük verilerini Kafka'dan okuyup işleyen ve 
PostgreSQL'e yazan streaming modüllerini içerir.
"""

from .pipeline import ElectricLoadPipeline
from .app import main

__all__ = ['ElectricLoadPipeline', 'main']