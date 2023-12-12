from application.module_abstract import AbstractModule
import mpl_scatter_density # adds projection='scatter_density'
from mpl_scatter_density import ScatterDensityArtist
from matplotlib import pyplot as plt
import matplotlib as mpl
from multiprocessing import Process
from matplotlib.colors import LinearSegmentedColormap
from astropy.visualization.mpl_normalize import ImageNormalize
from astropy.visualization import PowerStretch
import pandas as pd
import numpy as np
from astropy import units as u
import astropy.coordinates as coord
import os
import re

def analyze(numbers, input_dir, out1, out2, out3):
    parts = []
    for num in numbers:
        fin = os.path.join(input_dir, "gaia_data_dr3_task1_preprocessed_" + num)
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
    ax.set_title("Location")
    ax.set_xlabel("Right ascension [deg]")
    ax.set_ylabel("Declination [deg]")
    density = ax.scatter_density(df["ra"], df["dec"], cmap=white_viridis, norm=norm)
    fig.colorbar(density, label='')
    plt.savefig(out1)

    def fmt_func(x, pos):
        val = coord.Angle(-x*u.radian).wrap_at(360*u.deg).degree
        return f'${val:.0f}' + r'^{\circ}$'

    fig, ax = plt.subplots(subplot_kw=dict(projection="aitoff"))
    a = ScatterDensityArtist(ax,
                             df["ra_rad"],
                             df["dec_rad"],
                             cmap=white_viridis
                             )
    ax.add_artist(a)
    ticker = mpl.ticker.FuncFormatter(fmt_func)
    ax.xaxis.set_major_formatter(ticker)
    ax.set_xlabel('Right ascension [deg]')
    ax.set_ylabel('Declination [deg]')
    ax.grid(True)
    plt.savefig(out2)

    fig, ax = plt.subplots(subplot_kw=dict(projection="aitoff"))
    a = ScatterDensityArtist(ax,
                             df["l"],
                             df["b"],
                             cmap=white_viridis
                             )
    ax.add_artist(a)
    ticker = mpl.ticker.FuncFormatter(fmt_func)
    ax.xaxis.set_major_formatter(ticker)
    ax.set_xlabel('Galactic longitude [deg]')
    ax.set_ylabel('Galactic latitude [deg]')
    ax.grid()
    plt.savefig(out3)


class Task1(AbstractModule):
    def __init__(self, working_directory):
        self.set_working_directory(working_directory)


    def get_gaia_source_fields(self):
        return ["ra",
                "dec"]


    def get_gaia_source_restrictions(self):
        return ["ra IS NOT NULL",
                "dec IS NOT NULL"]


    def set_working_directory(self, dir):
        self.working_dir = dir
        self.preprocessed_data_dir = os.path.join(dir, "task1_preprocessed_data")
        self.results = os.path.join(dir, "task1_results")
        self.figure1 = os.path.join(self.results, "location.png")
        self.figure2 = os.path.join(self.results, "location_projection.png")
        self.figure3 = os.path.join(self.results, "location_galactic_coords_projection.png")
        if not os.path.exists( self.preprocessed_data_dir):
            os.makedirs( self.preprocessed_data_dir)
        if not os.path.exists(self.results):
            os.makedirs(self.results)


    def get_name(self):
        return "Расположение звезд"


    def get_preprocessed_data_dir(self):
        return  self.preprocessed_data_dir


    def get_preprocessed_data_number_by_filename(self, filename):
        if re.fullmatch(r'gaia_data_dr3_task1_preprocessed_\d{2}', filename) == None:
            return ""
        return filename.split("_")[-1]


    def preprocess_data(self, path, number):
        df = pd.read_csv(path, sep=' ', header = 0)
        radian = np.pi / 180
        df['ra_rad'] = df['ra'].apply(lambda x: (x if x <= 180 else x - 360) * radian)
        df['dec_rad'] = df['dec'].apply(lambda x: x * radian)
        alpha = df['ra_rad'].to_numpy()
        delta = df['dec_rad'].to_numpy()
        sin_const = np.sin(27.12825 * radian)
        cos_const = np.cos(27.12825 * radian)
        alpha_const = 192.85948 * radian
        b = np.arcsin(np.sin(delta) * sin_const + np.cos(delta) * cos_const * np.cos(alpha - alpha_const))
        cosll = (np.sin(delta) * cos_const - np.cos(delta) * sin_const * np.cos(alpha - alpha_const)) / np.cos(b)
        sinll = (np.cos(delta) * np.sin(alpha - alpha_const)) / np.cos(b)
        def lformula (cosll, sinll):
            return (np.arcsin(sinll) if cosll > 0 else np.pi - np.arcsin(sinll)) * -1 + 122.93192 * radian
        lvectorizer = np.vectorize(lformula)
        l = lvectorizer(cosll, sinll)
        normilize = np.vectorize(lambda x: -x if x <= np.pi else -x + np.pi * 2)
        l = normilize(l)
        df['l'] = l
        df['b'] = b
        df[['ra', 'ra_rad', 'dec', 'dec_rad', 'b', 'l']].to_csv(os.path.join(self.preprocessed_data_dir, "gaia_data_dr3_task1_preprocessed_" + number), index=False, sep = " ")


    def get_result_images_paths(self):
        return [self.figure1,
                self.figure2,
                self.figure3]


    def analyze_data(self, numbers):
        process = Process(target=analyze, args=(numbers, self.preprocessed_data_dir, self.figure1,
                                                self.figure2, self.figure3))
        process.start()
        process.join()