from langchain_openai import ChatOpenAI
import os
import logging
from dotenv import load_dotenv

load_dotenv(override=True)
logger = logging.getLogger("uvicorn.error")


class LLMSingleton:
    _instance = None
    _llm = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LLMSingleton, cls).__new__(cls)
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY is missing in environment variables")
            cls._llm = ChatOpenAI(
                temperature=0,
                model_name="gpt-4o-mini",
                openai_api_key=api_key
            )
            logger.info("✅ LLM initialized")
        return cls._instance

    def get_llm(self):
        return self._llm