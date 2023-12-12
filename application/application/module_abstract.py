from abc import ABC, abstractmethod

class AbstractModule(ABC):
    @abstractmethod
    def get_gaia_source_fields(self):
        pass

    @abstractmethod
    def get_gaia_source_restrictions(self):
        pass

    @abstractmethod
    def set_working_directory(self, dir):
        pass

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def get_preprocessed_data_dir(self):
        pass

    @abstractmethod
    def get_preprocessed_data_number_by_filename(self, filename):
        pass

    @abstractmethod
    def preprocess_data(self, path, number):
        pass

    @abstractmethod
    def get_result_images_paths(self):
        pass

    @abstractmethod
    def analyze_data(self, numbers):
        pass