from application.module_abstract import AbstractModule
import mpl_scatter_density # adds projection='scatter_density'
from matplotlib import pyplot as plt
from multiprocessing import Process
from matplotlib.colors import LinearSegmentedColormap
from astropy.visualization.mpl_normalize import ImageNormalize
from astropy.visualization import PowerStretch
import pandas as pd
import numpy as np
import os
import re

def analyze(numbers, input_dir, out1, out2):
    parts = []
    for num in numbers:
        fin = os.path.join(input_dir, "gaia_data_dr3_task6_preprocessed_" + num)
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
    ax.set_title("HR diargam")
    ax.set_xlabel(r'$G_{BP}-G_{RP}$')
    ax.set_ylabel(r'$M_{G}$')
    density = ax.scatter_density(df["bp_rp"], df["mg"], cmap=white_viridis, norm=norm)
    ax.invert_yaxis()
    fig.colorbar(density, label='')
    plt.savefig(out1)

    df = df.query("dist < 100 or (dist > 100 and azero_gspphot < 0.015 * 3.1) or azero_gspphot == '' or dist == ''")
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1, projection='scatter_density')
    ax.set_title("HR diargam (filtered data)")
    ax.set_xlabel(r'$G_{BP}-G_{RP}$')
    ax.set_ylabel(r'$M_{G}$')
    density = ax.scatter_density(df["bp_rp"], df["mg"], cmap=white_viridis, norm=norm)
    ax.invert_yaxis()
    fig.colorbar(density, label='')
    plt.savefig(out2)


class Task6(AbstractModule):
    def __init__(self, working_directory):
        self.set_working_directory(working_directory)


    def get_gaia_source_fields(self):
        return ["phot_g_mean_mag",
                "bp_rp",
                "azero_gspphot",
                "parallax"]


    def get_gaia_source_restrictions(self):
        return ["parallax_over_error > 1",
                "phot_g_mean_flux_over_error > 50",
                "phot_rp_mean_flux_over_error > 20",
                "phot_bp_mean_flux_over_error > 20",
                "phot_bp_rp_excess_factor < 1.3+0.06*power(phot_bp_mean_mag-phot_rp_mean_mag,2)",
                "phot_bp_rp_excess_factor > 1.0+0.015*power(phot_bp_mean_mag-phot_rp_mean_mag,2)",
                "visibility_periods_used > 8", "astrometric_chi2_al/(astrometric_n_good_obs_al-5) < 1.44*greatest(1,exp(-0.4*(phot_g_mean_mag-19.5)))"]
    

    def set_working_directory(self, dir):
        self.working_dir = dir
        self.preprocessed_data_dir = os.path.join(dir, "task6_preprocessed_data")
        self.results = os.path.join(dir, "task6_results")
        self.figure1 = os.path.join(self.results, "HR_diagram.png")
        self.figure2 = os.path.join(self.results, "HR_diagram_filtered.png")
        if not os.path.exists( self.preprocessed_data_dir):
            os.makedirs( self.preprocessed_data_dir)
        if not os.path.exists(self.results):
            os.makedirs(self.results)


    def get_name(self):
        return "Диаграмма Герцшпрунга-Рассела"


    def get_preprocessed_data_dir(self):
        return  self.preprocessed_data_dir


    def get_preprocessed_data_number_by_filename(self, filename):
        if re.fullmatch(r'gaia_data_dr3_task6_preprocessed_\d{2}', filename) == None:
            return ""
        return filename.split("_")[-1]


    def preprocess_data(self, path, number):
        df = pd.read_csv(path, sep=' ', header = 0)
        df['dist'] = df['parallax'].apply(lambda x: 1000.0 / x)
        g_mag = df['phot_g_mean_mag'].to_numpy()
        parallax = df['parallax'].to_numpy()
        df['mg'] = g_mag + 5 * np.log10(parallax / 1000.0) + 5
        df[['dist', 'mg', 'bp_rp', 'azero_gspphot']].to_csv(os.path.join(self.preprocessed_data_dir, "gaia_data_dr3_task6_preprocessed_" + number), index=False, sep = " ")
    

    def get_result_images_paths(self):
        return [self.figure1,
                self.figure2]


    def analyze_data(self, numbers):
        process = Process(target=analyze, args=(numbers, self.preprocessed_data_dir, self.figure1, self.figure2))
        process.start()
        process.join()