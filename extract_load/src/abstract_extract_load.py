from abc import ABC, abstractmethod


class AbstractExtractLoad(ABC):

    @abstractmethod
    def _get_data(self):
        pass

    @staticmethod
    @abstractmethod
    def _format_data(response):
        pass

    @abstractmethod
    def _post_data(self, data):
        pass
