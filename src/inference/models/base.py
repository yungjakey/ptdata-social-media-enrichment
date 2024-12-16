from abc import ABC, abstractmethod

from pydantic import BaseModel


class BaseInferenceModel(BaseModel, ABC):
    @staticmethod
    @abstractmethod
    def get_prompt() -> str:
        pass
