from application.module_abstract import AbstractModule
import mpl_scatter_density # adds projection='scatter_density'
from matplotlib import pyplot as plt
from multiprocessing import Process
from matplotlib.colors import LinearSegmentedColormap
from astropy.visualization.mpl_normalize import ImageNormalize
from astropy.visualization import PowerStretch
import pandas as pd
import os
import re

def analyze(numbers, input_dir, out):
    parts = []
    for num in numbers:
        fin = os.path.join(input_dir, "gaia_data_dr3_task2_preprocessed_" + num)
        parts.append(pd.read_csv(fin, sep=" ", header=0))
    df = pd.concat(parts, ignore_index=True)
    parts.clear()
    white_viridis = LinearSegmentedColormap.from_list('white_viridis', [
        (0, '#ffffff'),
        (1e-20, '#000000'),
        (0.1, '#440053'),
        (0.2, '#404388'),
        (0.4, '#2a788e'),
        (0.6, '#21a784'),
        (0.8, '#78d151'),
        (1, '#fde624'),
    ], N=256)

    norm = ImageNormalize(vmin=1, stretch=PowerStretch(0.5))

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1, projection='scatter_density')
    ax.set_title("Variation of proper motion")
    ax.set_xlabel("Proper motion in right ascension [mas/yr]")
    ax.set_ylabel("Proper motion in declination [mas/yr]")
    density = ax.scatter_density(df["pmra"], df["pmdec"], cmap=white_viridis, norm=norm)
    ax.set_xlim(-100, 100)
    ax.set_ylim(-100, 100)
    fig.colorbar(density, label='')
    plt.savefig(out)


class Task2(AbstractModule):
    def __init__(self, working_directory):
        self.set_working_directory(working_directory)


    def get_gaia_source_fields(self):
        return ["pmra",
                "pmdec"]


    def get_gaia_source_restrictions(self):
        return ["pmra IS NOT NULL",
                "pmdec IS NOT NULL"]
    

    def set_working_directory(self, dir):
        self.working_dir = dir
        self.preprocessed_data_dir = os.path.join(dir, "task2_preprocessed_data")
        self.results = os.path.join(dir, "task2_results")
        self.figure = os.path.join(self.results, "variation_of_proper_motion.png")
        if not os.path.exists( self.preprocessed_data_dir):
            os.makedirs( self.preprocessed_data_dir)
        if not os.path.exists(self.results):
            os.makedirs(self.results)


    def get_name(self):
        return "Движение звезд"


    def get_preprocessed_data_dir(self):
        return  self.preprocessed_data_dir


    def get_preprocessed_data_number_by_filename(self, filename):
        if re.fullmatch(r'gaia_data_dr3_task2_preprocessed_\d{2}', filename) == None:
            return ""
        return filename.split("_")[-1]


    def preprocess_data(self, path, number):
        df = pd.read_csv(path, sep=' ', header = 0)
        df[['pmra', 'pmdec']].to_csv(os.path.join(self.preprocessed_data_dir, "gaia_data_dr3_task2_preprocessed_" + number), index=False, sep = " ")
    

    def get_result_images_paths(self):
        return [self.figure]


    def analyze_data(self, numbers):
        process = Process(target=analyze, args=(numbers, self.preprocessed_data_dir, self.figure))
        process.start()
        process.join()