from application.module_abstract import AbstractModule
from matplotlib import pyplot as plt
from multiprocessing import Process
import pandas as pd
import os
import re

def analyze(numbers, input_dir, out1, out2):
    parts = []
    for num in numbers:
        fin = os.path.join(input_dir, "gaia_data_dr3_task3_preprocessed_" + num)
        parts.append(pd.read_csv(fin, sep=" ", header=0))
    df = pd.concat(parts, ignore_index=True)
    parts.clear()
    plt.figure()
    plt.hist(df["dist"], bins=100)
    plt.title("Distribution by distance")
    plt.xlabel("Distance [pc]")
    plt.ylabel("Count")
    plt.grid(True)
    plt.savefig(out1)

    plt.figure()
    plt.hist(df["dist"], bins=100)
    plt.title("Distribution by distance")
    plt.xlabel("Distance [pc]")
    plt.ylabel("Log(count)")
    plt.yscale("log")
    plt.grid(True)
    plt.savefig(out2)


class Task3(AbstractModule):
    def __init__(self, working_directory):
        self.set_working_directory(working_directory)


    def get_gaia_source_fields(self):
        return ["parallax"]


    def get_gaia_source_restrictions(self):
        return ["parallax IS NOT NULL"]
    

    def set_working_directory(self, dir):
        self.working_dir = dir
        self.preprocessed_data_dir = os.path.join(dir, "task3_preprocessed_data")
        self.results = os.path.join(dir, "task3_results")
        self.figure1 = os.path.join(self.results, "distribution_by_distance.png")
        self.figure2 = os.path.join(self.results, "distribution_by_distance_log_count.png")
        if not os.path.exists(self.preprocessed_data_dir):
            os.makedirs(self.preprocessed_data_dir)
        if not os.path.exists(self.results):
            os.makedirs(self.results)


    def get_name(self):
        return "Распределение звезд по расстоянию"


    def get_preprocessed_data_dir(self):
        return self.preprocessed_data_dir


    def get_preprocessed_data_number_by_filename(self, filename):
        if re.fullmatch(r'gaia_data_dr3_task3_preprocessed_\d{2}', filename) == None:
            return ""
        return filename.split("_")[-1]


    def preprocess_data(self, path, number):
        df = pd.read_csv(path, sep=' ', header = 0)
        df['dist'] = df['parallax'].apply(lambda x: 1000.0 / x)
        df[['dist']].to_csv(os.path.join(self.preprocessed_data_dir, "gaia_data_dr3_task3_preprocessed_" + number), index=False, sep = " ")


    def get_result_images_paths(self):
        return [self.figure1,
                self.figure2]


    def analyze_data(self, numbers):
        process = Process(target=analyze, args=(numbers, self.preprocessed_data_dir, self.figure1, self.figure2))
        process.start()
        process.join()