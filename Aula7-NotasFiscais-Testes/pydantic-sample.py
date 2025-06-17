from pydantic_ai import Agent
from pydantic_ai.models.gemini import GeminiModel
from pydantic_ai.providers.google_gla import GoogleGLAProvider
from dotenv import load_dotenv
import os

load_dotenv()

model = GeminiModel(
    'gemini-1.5-flash', provider=GoogleGLAProvider(api_key=os.environ.get('gem_key'))
)

agent = Agent(
    model,
    system_prompt='Be concise, reply with one sentence.',
)

result = agent.run_sync('Where does "hello world" come from?')
print(result.output)
"""
The first known use of "hello, world" was in a 1974 textbook about the C programming language.
"""