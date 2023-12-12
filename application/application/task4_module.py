from application.module_abstract import AbstractModule
from matplotlib import pyplot as plt
from multiprocessing import Process
import pandas as pd
import os
import re

def analyze(numbers, input_dir, name, out1, title1, xlabel1, ylabel1, out2, title2, xlabel2, ylabel2):
    parts = []
    for num in numbers:
        fin = os.path.join(input_dir, "gaia_data_dr3_task4_preprocessed_" + num)
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


def analyze_all(numbers, preprocessed_data_dir, figure1, figure2, figure3, figure4, figure5, figure6):
    analyze(numbers, preprocessed_data_dir, "phot_g_mean_mag",
            figure1, "Distribution by G magnitude", "G magnitude", "Count",
            figure2, "Distribution by G magnitude", "G magnitude", "Log(count)")
    analyze(numbers, preprocessed_data_dir, "phot_bp_mean_mag",
            figure3, "Distribution by BP magnitude", "BP magnitude", "Count",
            figure4, "Distribution by BP magnitude", "BP magnitude", "Log(count)")
    analyze(numbers, preprocessed_data_dir, "phot_rp_mean_mag",
            figure5, "Distribution by RP magnitude", "RP magnitude", "Count",
            figure6, "Distribution by RP magnitude", "RP magnitude", "Log(count)")


class Task4(AbstractModule):
    def __init__(self, working_directory):
        self.set_working_directory(working_directory)


    def get_gaia_source_fields(self):
        return ["phot_g_mean_mag",
                "phot_rp_mean_mag",
                "phot_bp_mean_mag"]


    def get_gaia_source_restrictions(self):
        return ["phot_g_mean_mag IS NOT NULL",
                "phot_rp_mean_mag IS NOT NULL",
                "phot_bp_mean_mag IS NOT NULL"]
    

    def set_working_directory(self, dir):
        self.working_dir = dir
        self.preprocessed_data_dir = os.path.join(dir, "task4_preprocessed_data")
        self.results = os.path.join(dir, "task4_results")
        self.figure1 = os.path.join(self.results, "distribution_by_g_mag.png")
        self.figure2 = os.path.join(self.results, "distribution_by_g_mag_log_count.png")
        self.figure3 = os.path.join(self.results, "distribution_by_bp_mag.png")
        self.figure4 = os.path.join(self.results, "distribution_by_bp_mag_log_count.png")
        self.figure5 = os.path.join(self.results, "distribution_by_rp_mag.png")
        self.figure6 = os.path.join(self.results, "distribution_by_rp_mag_log_count.png")
        if not os.path.exists( self.preprocessed_data_dir):
            os.makedirs( self.preprocessed_data_dir)
        if not os.path.exists(self.results):
            os.makedirs(self.results)


    def get_name(self):
        return "Распледеление звезд по магнитуде в разных спектрах"


    def get_preprocessed_data_dir(self):
        return  self.preprocessed_data_dir


    def get_preprocessed_data_number_by_filename(self, filename):
        if re.fullmatch(r'gaia_data_dr3_task4_preprocessed_\d{2}', filename) == None:
            return ""
        return filename.split("_")[-1]


    def preprocess_data(self, path, number):
        df = pd.read_csv(path, sep=' ', header = 0)
        df[['phot_g_mean_mag', 'phot_rp_mean_mag', 'phot_bp_mean_mag']].to_csv(os.path.join(self.preprocessed_data_dir, "gaia_data_dr3_task4_preprocessed_" + number), index=False, sep = " ")


    def get_result_images_paths(self):
        return [self.figure1,
                self.figure2,
                self.figure3,
                self.figure4,
                self.figure5,
                self.figure6]


    def analyze_data(self, numbers):
        process = Process(target=analyze_all, args=(numbers, self.preprocessed_data_dir, self.figure1,
                                                    self.figure2, self.figure3, self.figure4, self.figure5,
                                                    self.figure6))
        process.start()
        process.join()