# config.py
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()
DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
