# backend/config.py
import os
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# --- MISSING VARIABLE ADDED HERE ---
MODEL = "qwen2.5:7b-instruct"
# -----------------------------------

# ADX Settings
ADX_CLUSTER_URL = os.getenv("ADX_CLUSTER_URL")
ADX_DATABASE = os.getenv("ADX_DATABASE")

# Ollama Settings
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_CHAT_URL = f"{OLLAMA_BASE_URL}/api/chat"
OLLAMA_GENERATE_URL = f"{OLLAMA_BASE_URL}/api/generate"

# Authentication Settings
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")