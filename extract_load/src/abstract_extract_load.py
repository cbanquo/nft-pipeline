from abc import ABC, abstractmethod


class AbstractExtractLoad(ABC):

    @staticmethod
    @abstractmethod
    def _get_data():
        pass

    @staticmethod
    @abstractmethod
    def _format_data(data):
        pass

    @staticmethod
    @abstractmethod
    def _insert_data(data):
        pass
