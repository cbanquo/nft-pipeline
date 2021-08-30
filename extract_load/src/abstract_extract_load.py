from abc import ABC, abstractmethod


class AbstractExtractLoad(ABC):

    @staticmethod
    @abstractmethod
    def _get_data(self):
        pass

    @staticmethod
    @abstractmethod
    def _format_data(response):
        pass

    @staticmethod
    @abstractmethod
    def _insert_data(self, data):
        pass
