from astropy.io import ascii
from astroquery.gaia import Gaia
from tkinter import ttk
from tkinter import *
from application.module_abstract import AbstractModule
from threading import Thread
import os
import re

class DataLoader(ttk.Frame):
    def __init__(self, master, working_dir, modules: list[AbstractModule]):
        super().__init__(master)
        self.data_parts_num = 100
        self.general_entries_count = 1811709771
        self.modules = modules
        self.on_update_loaded_callbacks = []

        self.row1 = ttk.Frame(self)
        self.row1.pack(expand=1, fill=BOTH, side=TOP)

        self.col1 = ttk.Frame(self.row1)
        self.col1.pack(expand=1, fill=BOTH, side=LEFT)
        self.label1 = Label(self.col1, text="Загруженные фрагменты данных")
        self.label1.pack(expand=0, fill=X, side=TOP)
        self.list_loaded = Listbox(self.col1, selectmode=EXTENDED)
        self.list_loaded.pack(expand=1, fill=BOTH, side=BOTTOM)

        self.col2 = ttk.Frame(self.row1)
        self.col2.pack(expand=1, fill=BOTH, side=RIGHT)
        self.label2 = Label(self.col2, text="Доступные фрагменты данных")
        self.label2.pack(expand=0, fill=X, side=TOP)
        self.list_not_loaded = Listbox(self.col2, selectmode=EXTENDED)
        self.list_not_loaded.pack(expand=1, fill=BOTH, side=BOTTOM)

        self.row2 = ttk.Frame(self)
        self.row2.pack(expand=0, fill=X, side=BOTTOM)
        self.load_button = Button(self.row2, text="Загрузить выбранные доступные фрагменты", command=lambda: Thread(target=self.load_data).start())
        self.load_button.pack(side=RIGHT, padx=4, pady=4)

        self.set_working_directory(working_dir)


    def add_on_update_loaded_callback(self, callback):
        self.on_update_loaded_callbacks.append(callback)


    def set_working_directory(self, dir):
        self.working_dir = os.path.join(dir, "loaded_data")
        self.update_loaded()


    def get_loaded_data_dir(self):
        return self.working_dir
    

    def get_loaded_data_number_by_filename(self, filename):
        if re.fullmatch(r'gaia_data_dr3_\d{2}', filename) == None:
            return ""
        return filename.split("_")[-1]


    def get_loaded_data_filename_by_number(self, number):
        return os.path.join(self.working_dir, "gaia_data_dr3_" + number)


    def update_loaded(self):
        loaded = []
        not_loaded = [("0" + str(i)) if i < 10 else str(i) for i in range(self.data_parts_num)]
        if os.path.exists(self.working_dir):
            for filename in os.listdir(self.working_dir):
                if re.fullmatch(r'gaia_data_dr3_\d{2}', filename) == None:
                    continue
                number = filename.split("_")[-1]
                if number in not_loaded:
                    not_loaded.remove(number)
                    loaded.append(number)

        self.list_loaded.delete(0, self.list_loaded.size() - 1)
        for l in loaded:
            self.list_loaded.insert(END, " " + l)
        self.list_not_loaded.delete(0, self.list_not_loaded.size() - 1)
        for nl in not_loaded:
            self.list_not_loaded.insert(END, " " + nl)
        
        for callback in self.on_update_loaded_callbacks:
            callback()


    def load_data(self):
        selected = self.list_not_loaded.curselection()
        parts = [int(self.list_not_loaded.get(i)) for i in selected]
        if len(selected) > 0:
            if not os.path.exists(self.working_dir):
                os.makedirs(self.working_dir)
            self.load_button["state"] = "disabled"
            self.load_button.update()
            if len(selected) == 1:
                progress_bar = ttk.Progressbar(self.row2, orient=HORIZONTAL, mode="indeterminate")
                progress_bar.pack(expand=1, fill=X, side=LEFT, padx=4, pady=4)
                progress_bar.start()
            else:
                progress_bar = ttk.Progressbar(self.row2, orient=HORIZONTAL, mode="determinate", maximum=len(selected), value=0)
                progress_bar.pack(expand=1, fill=X, side=LEFT, padx=4, pady=4)
            progress_bar.update()
            for p in parts:
                job = Gaia.launch_job_async(self.get_query(p))
                results = job.get_results()
                filename = os.path.join(self.working_dir, "gaia_data_dr3_" + ("0" + str(p) if p < 10 else str(p)))
                ascii.write(results, filename, overwrite=True)
                if len(selected) > 1:
                    progress_bar.step()
            if len(selected) == 1:
                progress_bar.stop()
            progress_bar.pack_forget()
            self.update_loaded()
            self.load_button["state"] = "normal"
            self.load_button.update()


    def get_query(self, part_num):
        query = f"SELECT "
        fields = []
        restrictions = []
        for module in self.modules:
            fields += module.get_gaia_source_fields()
            restrictions += module.get_gaia_source_restrictions()
        query += f", ".join(set(fields))
        query += f" FROM gaiadr3.gaia_source WHERE "
        query += f" AND ".join(set(restrictions))
        query += f" AND random_index BETWEEN {int(part_num / self.data_parts_num * self.general_entries_count)} AND {int((part_num + 1) / self.data_parts_num * self.general_entries_count) - 1}"
        return query