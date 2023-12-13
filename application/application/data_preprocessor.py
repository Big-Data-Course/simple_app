from tkinter import ttk
from tkinter import *
from application.module_abstract import AbstractModule
from application.data_loader import DataLoader
from threading import Thread
import os

class DataPreprocessor(ttk.Frame):
    def __init__(self, master, data_loader: DataLoader, module: AbstractModule):
        super().__init__(master)
        self.data_loader = data_loader
        self.module = module
        self.on_update_preprocessed_callbacks = []

        self.row1 = ttk.Frame(self)
        self.row1.pack(expand=1, fill=BOTH, side=TOP)

        self.col1 = ttk.Frame(self.row1)
        self.col1.pack(expand=1, fill=BOTH, side=LEFT)
        self.label1 = Label(self.col1, text="Обработанные фрагменты данных")
        self.label1.pack(expand=0, fill=X, side=TOP)
        self.list_preprocessed = Listbox(self.col1, selectmode=EXTENDED)
        self.list_preprocessed.pack(expand=1, fill=BOTH, side=BOTTOM)

        self.col2 = ttk.Frame(self.row1)
        self.col2.pack(expand=1, fill=BOTH, side=RIGHT)
        self.label2 = Label(self.col2, text="Загруженные фрагменты данных")
        self.label2.pack(expand=0, fill=X, side=TOP)
        self.list_not_preprocessed = Listbox(self.col2, selectmode=EXTENDED)
        self.list_not_preprocessed.pack(expand=1, fill=BOTH, side=BOTTOM)

        self.row2 = ttk.Frame(self)
        self.row2.pack(expand=0, fill=X, side=BOTTOM)
        self.preprocess_button = Button(self.row2, text="Обработать выбранные фрагменты", command=lambda: Thread(target=self.preprocess_data).start())
        self.preprocess_button.pack(side=RIGHT, padx=4, pady=4)

        self.update_preprocessed()
        self.data_loader.add_on_update_loaded_callback(self.update_preprocessed)


    def add_on_update_preprocessed_callback(self, callback):
        self.on_update_preprocessed_callbacks.append(callback)


    def update_preprocessed(self):
        preprocessed = []
        not_preprocessed = []
        if os.path.exists(self.data_loader.get_loaded_data_dir()):
            for filename in os.listdir(self.data_loader.get_loaded_data_dir()):
                number = self.data_loader.get_loaded_data_number_by_filename(filename)
                if number:
                    not_preprocessed.append(number)

        if os.path.exists(self.module.get_preprocessed_data_dir()):
            for filename in os.listdir(self.module.get_preprocessed_data_dir()):
                number = self.module.get_preprocessed_data_number_by_filename(filename)
                if number in not_preprocessed:
                    not_preprocessed.remove(number)
                    preprocessed.append(number)

        self.list_preprocessed.delete(0, self.list_preprocessed.size() - 1)
        for p in preprocessed:
            self.list_preprocessed.insert(END, " " + p)
        self.list_not_preprocessed.delete(0, self.list_not_preprocessed.size() - 1)
        for np in not_preprocessed:
            self.list_not_preprocessed.insert(END, " " + np)

        for callback in self.on_update_preprocessed_callbacks:
            callback()


    def preprocess_data(self):
        selected = [self.list_not_preprocessed.get(i) for i in self.list_not_preprocessed.curselection()]
        if len(selected) > 0:
            self.preprocess_button["state"] = "disabled"
            self.preprocess_button.update()
            if len(selected) == 1:
                progress_bar = ttk.Progressbar(self.row2, orient=HORIZONTAL, mode="indeterminate")
                progress_bar.pack(expand=1, fill=X, side=LEFT, padx=4, pady=4)
                progress_bar.start()
            else:
                progress_bar = ttk.Progressbar(self.row2, orient=HORIZONTAL, mode="determinate", maximum=len(selected), value=0)
                progress_bar.pack(expand=1, fill=X, side=LEFT, padx=4, pady=4)
            progress_bar.update()
            for s in selected:
                self.module.preprocess_data(self.data_loader.get_loaded_data_filename_by_number(s[1:]), s[1:])
                if len(selected) > 1:
                    progress_bar.step()
            if len(selected) == 1:
                progress_bar.stop()
            progress_bar.pack_forget()
            self.update_preprocessed()
            self.preprocess_button["state"] = "normal"
            self.preprocess_button.update()