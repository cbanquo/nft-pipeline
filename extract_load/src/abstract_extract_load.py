from abc import ABC, abstractmethod


class AbstractExtractLoad(ABC):

    def run(self) -> None:
        """Run extract and load pipeline
        """
        response = self._make_request()
        f_response = self._format_response(response)
        self._load_data(f_response)

    def _make_request(self):
        pass

    @abstractmethod
    def _format_response(response):
        pass

    def _load_data(self, data):
        pass
