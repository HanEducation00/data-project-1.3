#!/usr/bin/env python3
# Ana başlatma scripti

import sys
import os

# Ana dizini belirle
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Modülleri import etmeden önce import mekanizmasını ayarla
from streaming.app import main

if __name__ == "__main__":
    main()
