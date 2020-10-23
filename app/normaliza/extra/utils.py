
import os
import csv
import traceback
from django.core.files.storage import FileSystemStorage
from django.core.files.base import ContentFile
import glob

class InteractiveLog:
    user_id = 0
    def __init__(self, user_id, execution_id=None, delimiter='|'):
        self.logs_clean = []
        self.logs_exec = []

        self.clean_file = os.path.join('media', user_id, 'logs', 'Log_alistamiento.csv')
        self.exec_file = os.path.join('media', user_id, 'logs', 'Log_ejecucion.csv')
        self.error_file = os.path.join('media', user_id, 'logs', 'error.log')
        self.input_file = os.path.join('media', user_id, 'input', 'a')
        self.ouput_file = os.path.join('media', user_id, 'output', 'a')
        self.delimiter = delimiter

        self.user_id = user_id
        # Al instanciar la clase, no se sabe como van a quedar los encabezados de los CSV.
        # Estos flags indican si se debe agregar el header.
        self.clean_header = False
        self.exec_header = False

        fs = FileSystemStorage(f'media/{user_id}/logs')
        if not fs.exists('a'):
            alistamiento = ContentFile("Iniciando log de alistamiento")
            fs.save('a', alistamiento)
            fs.delete('a')

        fs = FileSystemStorage(f'media/{user_id}/input')
        if not fs.exists('a'):
            alistamiento = ContentFile("Iniciando log de alistamiento")
            fs.save('a', alistamiento)
            fs.delete('a')

        fs = FileSystemStorage(f'media/{user_id}/ouput')
        if not fs.exists('a'):
            alistamiento = ContentFile("Iniciando log de alistamiento")
            fs.save('a', alistamiento)
            fs.delete('a')



    def append_clean(self, obj):
        self.write_row(self.clean_file, obj, 'clean_header')

    def append_exec(self, obj):
        self.write_row(self.exec_file, obj, 'exec_header')

    def write_row(self, file_path, obj_dict: dict, header_flag: str):
        fieldnames = list(obj_dict.keys())



        with open(file_path, 'a', encoding='windows-1252') as f, open(self.error_file, 'a', encoding='windows-1252') as err:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=self.delimiter)
            if not getattr(self, header_flag):
                setattr(self, header_flag, True)
                writer.writeheader()
            try:
                writer.writerow(obj_dict)
            except Exception as e:
                err.write(f'EXCEPTION WRITTING LOG ROW: {obj_dict}\r\n')
                traceback.print_exc(file=err)


def get_interactive_log(user_id, execution_id=None):
    return InteractiveLog(user_id, execution_id)
