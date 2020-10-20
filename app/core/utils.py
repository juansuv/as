import os
import csv
import traceback
from threading import Thread

from django.core.mail import EmailMessage

from app.settings import DEFAULT_FROM_EMAIL, BCC_EMAILS, MEDIA_ROOT


class InteractiveLog:
    def __init__(self, model_name, execution_id=None, delimiter='|'):
        self.logs_clean = []
        self.logs_exec = []

        self.clean_file = os.path.join(MEDIA_ROOT, model_name, 'logs', 'Log_alistamiento.csv')
        self.exec_file = os.path.join(MEDIA_ROOT, model_name, 'logs', 'Log_ejecucion.csv')
        self.error_file = os.path.join(MEDIA_ROOT, model_name, 'logs', 'error.log')
        self.delimiter = delimiter

        # Al instanciar la clase, no se sabe como van a quedar los encabezados de los CSV.
        # Estos flags indican si se debe agregar el header.
        self.clean_header = False
        self.exec_header = False

        open(self.clean_file, 'w').close()
        open(self.exec_file, 'w').close()

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


def get_interactive_log(model_name, execution_id=None):
    return InteractiveLog(model_name, execution_id)


class EmailThread(Thread):
    def __init__(self, subject, html_content, recipient_list, files=None):
        self.subject = subject
        self.recipient_list = recipient_list
        self.html_content = html_content
        self.files = files
        Thread.__init__(self)

    def run(self):
        msg = EmailMessage(self.subject, self.html_content, DEFAULT_FROM_EMAIL, self.recipient_list, bcc=BCC_EMAILS)
        if self.files is not None:
            for file in self.files:
                msg.attach(file.name, file.read(), file.content_type)

        msg.content_subtype = "html"
        msg.send()


def send_html_mail(subject, html_content, recipient_list, files=None):
    EmailThread(subject, html_content, recipient_list, files).start()
