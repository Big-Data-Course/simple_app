from application.module_abstract import AbstractModule
from matplotlib import pyplot as plt
from multiprocessing import Process
import pandas as pd
import os
import re

def analyze(numbers, input_dir, name, out1, title1, xlabel1, ylabel1, out2, title2, xlabel2, ylabel2):
    parts = []
    for num in numbers:
        fin = os.path.join(input_dir, "gaia_data_dr3_task5_preprocessed_" + num)
        parts.append(pd.read_csv(fin, sep=" ", header=0))
    df = pd.concat(parts, ignore_index=True)
    parts.clear()
    plt.figure()
    plt.hist(df[name], bins=100)
    plt.title(title1)
    plt.xlabel(xlabel1)
    plt.ylabel(ylabel1)
    plt.grid(True)
    plt.savefig(out1)

    plt.figure()
    plt.hist(df[name], bins=100)
    plt.title(title2)
    plt.xlabel(xlabel2)
    plt.ylabel(ylabel2)
    plt.yscale("log")
    plt.grid(True)
    plt.savefig(out2)


def analyze_all(numbers, preprocessed_data_dir, figure1, figure2, figure3, figure4):
    analyze(numbers, preprocessed_data_dir, "visibility_periods_used",
            figure1, "Distribution by visibility periods", "Visibility periods count", "Count",
            figure2, "Distribution by visibility periods", "Visibility periods count", "Log(count)")
    analyze(numbers, preprocessed_data_dir, "astrometric_matched_transits",
            figure3, "Distribution by matched transits", "Matched transits count", "Count",
            figure4, "Distribution by matched transits", "Matched transits count", "Log(count)")


class Task5(AbstractModule):
    def __init__(self, working_directory):
        self.set_working_directory(working_directory)


    def get_gaia_source_fields(self):
        return ["visibility_periods_used",
                "astrometric_matched_transits"]


    def get_gaia_source_restrictions(self):
        return ["visibility_periods_used IS NOT NULL",
                "astrometric_matched_transits IS NOT NULL"]
    

    def set_working_directory(self, dir):
        self.working_dir = dir
        self.preprocessed_data_dir = os.path.join(dir, "task5_preprocessed_data")
        self.results = os.path.join(dir, "task5_results")
        self.figure1 = os.path.join(self.results, "distribution_visibility_periods_used.png")
        self.figure2 = os.path.join(self.results, "distribution_visibility_periods_used_log_count.png")
        self.figure3 = os.path.join(self.results, "distribution_by_astrometric_matched_transits.png")
        self.figure4 = os.path.join(self.results, "distribution_by_astrometric_matched_transits_log_count.png")
        if not os.path.exists( self.preprocessed_data_dir):
            os.makedirs( self.preprocessed_data_dir)
        if not os.path.exists(self.results):
            os.makedirs(self.results)


    def get_name(self):
        return "Распределение звезд по числу наблюдений"


    def get_preprocessed_data_dir(self):
        return  self.preprocessed_data_dir


    def get_preprocessed_data_number_by_filename(self, filename):
        if re.fullmatch(r'gaia_data_dr3_task5_preprocessed_\d{2}', filename) == None:
            return ""
        return filename.split("_")[-1]


    def preprocess_data(self, path, number):
        df = pd.read_csv(path, sep=' ', header = 0)
        df[['visibility_periods_used', 'astrometric_matched_transits']].to_csv(os.path.join(self.preprocessed_data_dir, "gaia_data_dr3_task5_preprocessed_" + number), index=False, sep = " ")
    

    def get_result_images_paths(self):
        return [self.figure1,
                self.figure2,
                self.figure3,
                self.figure4]


    def analyze_data(self, numbers):
        process = Process(target=analyze_all, args=(numbers, self.preprocessed_data_dir, self.figure1,
                                                    self.figure2, self.figure3, self.figure4))
        process.start()
        process.join()