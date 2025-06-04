# src/create_tables/create_sector_table.py
"""
📁 File: create_sector_table.py  
🎯 Purpose: Create the `sector_data` table in the Railway PostgreSQL DB using its SQLAlchemy model  
📥 Input: SectorData class (from models.py), DATABASE_URL  
📤 Output: A single table named `sector_data` created in the database  
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine
from DBintegration.models import Base, SectorData, DATABASE_URL

# ⚙️ יצירת engine
engine = create_engine(DATABASE_URL)

# 🧱 יצירת טבלת sector_data בלבד (ללא שאר הטבלאות)
Base.metadata.create_all(bind=engine, tables=[SectorData.__table__])

print("✅ טבלת sector_data נוצרה בהצלחה לפי ההגדרה ב־models.py")
