from abc import ABC, abstractmethod


class AbstractPipeline(ABC):

    @staticmethod
    @abstractmethod
    def run():
        pass

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
