from tkinter import ttk
from tkinter import filedialog as fd
from tkinter import *
from application.data_loader import DataLoader
from application.data_preprocessor import DataPreprocessor
from application.data_analyzer import DataAnalyzer
from application.task1_module import Task1
from application.task2_module import Task2
from application.task3_module import Task3
from application.task4_module import Task4
from application.task5_module import Task5
from application.task6_module import Task6
import os

class App(Tk):
    def __init__(self):
        super().__init__()
        self.title("Анализ данных обзора телескопа \"Gaia\"")
        self.geometry('720x540')

        self.settings_filename = "settings.txt"
        self.working_dir = ""
        self.load_settings()
        self.modules = [Task1(self.working_dir),
                        Task2(self.working_dir),
                        Task3(self.working_dir),
                        Task4(self.working_dir),
                        Task5(self.working_dir),
                        Task6(self.working_dir)]
        self.set_tab_widget()
        self.set_warning()


    def load_settings(self):
        if os.path.exists(self.settings_filename):
            with open(self.settings_filename, "r") as settings:
                for line in settings.readlines():
                    elements = line.split(" ")
                    if len(elements) != 2:
                        continue
                    name, value = elements
                    if name == "working_directory" and os.path.exists(value):
                        self.working_dir = value

    
    def set_warning(self):
        if not self.working_dir:
            self.warning_frame = ttk.Frame(self, padding=4)
            self.warning_frame.pack(fill=X, side=TOP)
            self.warning_label = Label(self.warning_frame, text="Предупреждение: Рабочая директория не задана")
            self.warning_label.pack(side=LEFT)
            self.set_dir_button = Button(self.warning_frame, text="Задать рабочую директорию", command=self.set_working_directory)
            self.set_dir_button.pack(side=RIGHT)


    def set_working_directory(self):
        dir = fd.askdirectory(title="Выбрать рабучую директорию")
        if dir:
            self.warning_frame.pack_forget()
            self.working_dir = dir
            self.tab1.set_working_directory(self.working_dir)
            for module in self.modules:
                module.set_working_directory(self.working_dir)


    def set_tab_widget(self):
        self.tab_widget = ttk.Notebook(self)
        self.tab_widget.pack(expand=1, fill=BOTH)
        self.tab1 = DataLoader(self.tab_widget, self.working_dir, self.modules)
        self.tab_widget.add(self.tab1, text="Загрузка данных")
        self.tab2 = ttk.Frame(self.tab_widget)
        self.tab_widget.add(self.tab2, text="Предварительная обработка данных")
        self.tab_widget2 = ttk.Notebook(self.tab2)
        self.tab_widget2.pack(expand=1, fill=BOTH)
        preprocessors = {}
        for module in self.modules:
            module_frame = DataPreprocessor(self.tab_widget2, self.tab1, module)
            preprocessors[module.get_name()] = module_frame
            self.tab_widget2.add(module_frame, text=module.get_name())
        self.tab3 = ttk.Frame(self.tab_widget)
        self.tab_widget.add(self.tab3, text="Анализ данных и визуализация")
        self.tab_widget3 = ttk.Notebook(self.tab3)
        self.tab_widget3.pack(expand=1, fill=BOTH)
        for module in self.modules:
            module_frame = DataAnalyzer(self.tab_widget3, preprocessors[module.get_name()], module)
            self.tab_widget3.add(module_frame, text=module.get_name())


    def save_settings(self):
        with open(self.settings_filename, "w") as settings:
            settings.writelines(f"working_directory {self.working_dir}")


    def __del__(self):
        self.save_settings()


def main():
    app = App()
    app.mainloop()
    del app