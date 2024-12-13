from abc import ABC, abstractmethod

from pydantic import BaseModel


class BaseInferenceModel(BaseModel, ABC):
    @abstractmethod
    @staticmethod
    def get_prompt() -> str:
        pass
