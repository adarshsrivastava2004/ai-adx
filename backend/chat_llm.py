import requests

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen2.5:7b-instruct"

SYSTEM_PROMPT = """
You are a friendly assistant.
Respond naturally to greetings and casual questions.
Be short and polite.
"""

def chat_llm(user_message: str) -> str:
    prompt = f"""
{SYSTEM_PROMPT}

User:
{user_message}
"""

    response = requests.post(
        OLLAMA_URL,
        json={"model": MODEL, "prompt": prompt, "stream": False},
        timeout=30
    )

    return response.json()["response"].strip()
