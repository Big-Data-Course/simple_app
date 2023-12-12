from tkinter import ttk
from tkinter import *
from application.module_abstract import AbstractModule
from application.data_preprocessor import DataPreprocessor
from threading import Thread
import os

class DataAnalyzer(ttk.Frame):
    def __init__(self, master, data_preprocessor: DataPreprocessor, module: AbstractModule):
        super().__init__(master)
        self.module = module
        self.data_preprocessor = data_preprocessor

        self.row1 = ttk.Frame(self)
        self.row1.pack(expand=1, fill=BOTH, side=TOP)

        self.col1 = ttk.Frame(self.row1)
        self.col1.pack(expand=0, fill=Y, side=LEFT)
        self.label1 = Label(self.col1, text="Фрагменты данных")
        self.label1.pack(expand=0, side=TOP)
        self.list_avaliable = Listbox(self.col1, selectmode=EXTENDED)
        self.list_avaliable.pack(expand=1, fill=Y, side=BOTTOM)

        self.col2 = ttk.Frame(self.row1)
        self.col2.pack(expand=1, fill=BOTH, side=RIGHT)
        self.label2 = Label(self.col2, text="Графики")
        self.label2.pack(expand=0, fill=X, side=TOP)
        self.row2 = ttk.Frame(self.col2)
        self.row2.pack(expand=1, fill=BOTH, side=BOTTOM)
        self.images_view = None
        self.scrollbar = None

        self.row3 = ttk.Frame(self)
        self.row3.pack(expand=0, fill=X, side=BOTTOM)
        self.process_button = Button(self.row3, text="Построить графики", command=lambda: Thread(target=self.analyze_data).start())
        self.process_button.pack(side=RIGHT, padx=4, pady=4)

        self.update_avaliable()
        self.update_images()
        self.data_preprocessor.add_on_update_preprocessed_callback(self.update_avaliable)


    def update_avaliable(self):
        avaliable = []
        for filename in os.listdir(self.module.get_preprocessed_data_dir()):
            number = self.module.get_preprocessed_data_number_by_filename(filename)
            if number:
                avaliable.append(number)

        self.list_avaliable.delete(0, self.list_avaliable.size() - 1)
        for p in avaliable:
            self.list_avaliable.insert(END, " " + p)


    def update_images(self):
        paths = self.module.get_result_images_paths()
        self.images = []
        for p in paths:
            if os.path.exists(p):
                self.images.append(PhotoImage(file=p))
        if self.images_view != None:
            self.images_view.pack_forget()
            self.scrollbar.pack_forget()
            self.images_view = None
            self.scrollbar = None
        if len(self.images) > 0:
            self.images_view = Text(self.row2, wrap="none")
            self.scrollbar = Scrollbar(self.row2, orient="vertical", command=self.images_view.yview)
            self.images_view.configure(yscrollcommand=self.scrollbar.set)
            self.scrollbar.pack(side=RIGHT, fill=Y)
            self.images_view.pack(side=LEFT, fill=BOTH, expand=1)
            self.images_view.tag_configure("center", justify="center")
            for i in self.images:
                self.images_view.image_create(END, image=i, align=CENTER)
                self.images_view.insert(END, "\n")
            self.images_view.tag_add("center", 1.0, END)
            self.images_view.configure(state="disabled")
            self.images_view.update()


    def analyze_data(self):
        selected = [self.list_avaliable.get(i)[1:] for i in self.list_avaliable.curselection()]
        if len(selected) > 0:
            self.process_button["state"] = "disabled"
            self.process_button.update()
            progress_bar = ttk.Progressbar(self.row3, orient=HORIZONTAL, mode="indeterminate")
            progress_bar.pack(expand=1, fill=X, side=LEFT, padx=4, pady=4)
            progress_bar.start()
            progress_bar.update()
            self.module.analyze_data(selected)
            progress_bar.stop()
            progress_bar.pack_forget()
            self.update_images()
            self.process_button["state"] = "normal"
            self.process_button.update()