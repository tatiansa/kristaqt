import configparser
import os
import sys
import zipfile
from abc import (
    ABCMeta,
    abstractmethod,
)
from datetime import datetime

import fdb
from PyQt6 import QtWidgets, uic
from PyQt6.QtCore import (
    QThreadPool,
    QObject,
    pyqtSignal,
    QRunnable,
    pyqtSlot,
)
from PyQt6.QtWidgets import (
    QApplication,
    QFileDialog,
    QMessageBox,
    QProgressBar,
)
from fdb import DatabaseError

from creators import (
    ArgEstCreator,
    ArgFkrCreator,
    ArgMainCreator,
    ArgOrgCreator,
    PbsFkrCreator,
    PbsMainCreator,
    PlpFkrCreator,
    PlpMainCreator,
    PlpOrgCreator,
)
from info_strings import DATABASE_CONNECTION, CREATE_MAIN, CREATE_FKR, CREATE_ORG, CREATE_ZIP, CREATE_EST
from krista_sql import (
    ARG_BANK_SQL,
    ARG_EST_SQL,
    ARG_ORG_SQL,
    ORG_INFO_SQL,
    PBS_SQL,
    PLP_IN_SQL,
    PLP_OUT_SQL,
    PLP_ACCOUNT_FILTER,
    PBS_ACCOUNT_FILTER,
    ARG_ACCOUNT_FILTER,
)
from settings import (
    ARG_CONFIG,
    DATABASE_DATE_FORMAT,
    DATE_FORMAT,
    INCOMING_SQL_ADDITION,
    OUTGOING_SQL_ADDITION,
    PBS_CONFIG, FKR_KEYS,
)


class DynamicConfigFile:
    """Класс файла конфигурации может быть только один, для удобства обращения.
    Отвечает за чтение и запись данных окна.

    Attributes:
        instance: объект класса, после инициализации при обращении всегда возвращается он
        config: парсер конфига
        host: хост
        login: имя пользователя
        password: пароль
        unload_dir: директория для выгрузки
        database_path: путь к базе данных
        date_begin: дата начала выгрузки
        date_end: дата завершения выгрузки
    """

    file_name = 'krista.ini'
    section_name = 'krista'

    instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.instance:
            cls.instance = super().__new__(cls, *args, **kwargs)

        return cls.instance

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.host = None
        self.login = None
        self.password = None
        self.unload_dir = None
        self.database_path = None
        self.filter = None
        self.date_begin = None
        self.date_end = None

    def read_item(self, item, section='krista'):
        result = None
        try:
            result = self.config[section][item]
        except KeyError:
            pass

        return result

    def exists(self):
        return os.path.isfile(os.getcwd() + os.sep + self.file_name)

    def read(self):
        if self.exists():
            self.config.read(self.file_name)
            self.host = self.read_item('host')
            self.login = self.read_item('login')
            self.password = self.read_item('password')
            self.unload_dir = self.read_item('unload_dir')
            self.database_path = self.read_item('database_path')
            self.filter = self.read_item('filter')
            self.date_begin = self.read_item('date_begin')
            self.date_end = self.read_item('date_end')

    def write(self, login, password, unload_dir, database_path, filter, date_begin, date_end, host='127.0.0.1'):
        config = configparser.ConfigParser()
        config.add_section('krista')
        config[self.section_name]['host'] = host
        config[self.section_name]['login'] = login
        config[self.section_name]['password'] = password
        config[self.section_name]['unload_dir'] = unload_dir
        config[self.section_name]['database_path'] = database_path
        config[self.section_name]['filter'] = filter
        config[self.section_name]['date_begin'] = date_begin.toString(DATE_FORMAT)
        config[self.section_name]['date_end'] = date_end.toString(DATE_FORMAT)

        with open(self.file_name, 'w') as config_file:
            config.write(config_file)


class DatabaseConnection:
    """Класс отвечает за соединение с базой данных, исполнение запросов и вывод данных в виде словаря.

        Attributes:
            cursor: курсор
            connection: соединение
            login: имя пользователя
            password: пароль
            database_path: путь в бд
            host: хост
            charset: кодировка

    """

    def __init__(self, login, password, database_path, host='127.0.0.1', charset='WIN1251'):
        self.cursor = None
        self.connection = None
        self.login = login
        self.password = password
        self.database_path = database_path
        self.host = host
        self.charset = charset
        self.connect()

    def connect(self):
        """Устанавливаем соединение с бд."""

        self.connection = fdb.connect(
            host=self.host,
            database=self.database_path,
            user=self.login,
            password=self.password,
            charset=self.charset,
        )

    def execute(self, sql):
        """Получение данных запроса и вывод в виде словаря.

        :param sql: запрос к бд
        :return: словарь вида название колонки: значение
        """

        self.cursor = self.connection.cursor()
        self.cursor.execute(sql)
        columns = [col[0] for col in self.cursor.description]
        result = [
            dict(zip(columns, row))
            for row in self.cursor.fetchall()
        ]
        return result


class WorkerSignals(QObject):
    """Сигналы нашего потока исполнения."""

    result = pyqtSignal(str)
    error = pyqtSignal(tuple)
    progress = pyqtSignal(tuple)
    set_progress_max = pyqtSignal(int)
    finished = pyqtSignal()


class WorkerWrapper(QRunnable):
    def __init__(self, runnable_object):
        super().__init__()
        self.runnable_object = runnable_object
        self.signals = WorkerSignals()
        self.runnable_object.progress_emiter = self.signals.progress
        self.runnable_object.progress_max_emiter = self.signals.set_progress_max

    @pyqtSlot()
    def run(self):
        try:
            result = self.runnable_object.run()
        except:
            exctype, value = sys.exc_info()[:2]
            self.signals.error.emit((exctype, value))
        else:
            self.signals.result.emit(result)
        finally:
            self.signals.finished.emit()


class UnloadAbs(metaclass=ABCMeta):
    """Базовый класс для общей логики выгрузки.
    Содержит обязательный метод run в котором должна содержаться логика создания файлов dbf

    Attributes:
        prefix: префикс названия zip файла
        dbf_files_names: список имен файлов которые необходимо включить в архив
        login: имя пользователя
        password: пароль
        unload_dir: директория выгрузки
        database_path: путь к бд
        date_begin: дата начала выгрузки
        date_end: дата завершения выгрузки
        filter: доп фильтрация запроса
    """

    prefix = None
    dbf_files_names = ()

    def __init__(self, login, password, unload_dir, database_path, date_begin, date_end, filter):
        self.login = login
        self.password = password
        self.unload_dir = unload_dir
        self.database_path = database_path
        self.date_begin = date_begin
        self.date_end = date_end
        self.filter = filter

        # в дальнейшем надо выделить сигналы прогресса из основного кода
        self.progress_emiter = None
        self.progress_max_emiter = None
        DynamicConfigFile().write(login, password, unload_dir, database_path, filter, date_begin, date_end)

    def statusbar_max_info(self):
        if self.progress_max_emiter:
            # +2 это соединение с базой и заталкивание в архив
            self.progress_max_emiter.emit(len(self.dbf_files_names) + 2)

    def step_info(self, message):
        if self.progress_emiter:
            self.progress_emiter.emit((message, 1))

    @abstractmethod
    def run(self):
        pass

    def zip_files(self):
        result = None
        current_date = datetime.now().strftime('%Y%m%d')
        zip_file_name = f'{self.prefix}_{current_date}.zip'

        main_ = os.getcwd()
        os.chdir(self.unload_dir)

        check_exists_function = os.path.isfile
        if all([check_exists_function(file_name) for file_name in self.dbf_files_names]):
            with zipfile.ZipFile(zip_file_name, 'w') as plp_zip_file:
                for dbf_file in self.dbf_files_names:
                    plp_zip_file.write(dbf_file)
                    os.remove(dbf_file)
            result = zip_file_name

        os.chdir(main_)

        return result


class PlpUnload(UnloadAbs):
    """Выгрузка платежных поручений."""

    prefix = 'plp'
    dbf_files_names = (PlpMainCreator.file_name, PlpFkrCreator.file_name, PlpOrgCreator.file_name)

    def __init__(self, login, password, unload_dir, database_path, date_begin, date_end, sql_filter):
        super().__init__(login, password, unload_dir, database_path, date_begin, date_end, sql_filter)
        self.organizations_ids = self.fkr_list = None

    def prepare_sql(self, blank, addition):
        """Подготавливаем запрос

        :param blank: основной запрос
        :param addition: дополнительные дынные из settings
        :return: запрос для передачи в бд
        """
        result = blank.format(
            self.date_begin.toString(DATABASE_DATE_FORMAT),
            self.date_end.toString(DATABASE_DATE_FORMAT),
        )
        if addition:
            result += addition

        if self.filter:
            result += PLP_ACCOUNT_FILTER.format(self.filter)

        return result

    def create_main(self, connection):
        """Подготавливаем запрос передаём, получаем данные из бд, создаем файл plp_main.dbf

        :param connection: соединение
        """
        outgoing_request = self.prepare_sql(
            PLP_OUT_SQL,
            OUTGOING_SQL_ADDITION,
        )
        db_records = connection.execute(outgoing_request)

        incoming_request = self.prepare_sql(
            PLP_IN_SQL,
            INCOMING_SQL_ADDITION,
        )
        db_records.extend(connection.execute(incoming_request))

        if db_records:
            main_creator = PlpMainCreator()
            main_creator.create(db_records, unload_dir=self.unload_dir)
            self.fkr_list = main_creator.fkr_list
            self.organizations_ids = main_creator.organizations_ids

    def create_org(self, connection):
        """Подготавливаем запрос передаём, получаем данные из бд, создаем файл plp_org.dbf

        :param connection: соединение
        """
        if self.organizations_ids:
            dest_org_request = ORG_INFO_SQL.format(tuple(self.organizations_ids))
            db_records = connection.execute(dest_org_request)
            org_creator = PlpOrgCreator()
            org_creator.create(db_records, unload_dir=self.unload_dir)

    def create_kfr(self):
        """Подготавливаем запрос передаём, получаем данные из бд, создаем файл plp_fkr.dbf

        :param connection: соединение
        """

        def prepare_record_fkr(data):
            result = [
                dict(zip(FKR_KEYS, fkr_item)) for fkr_item in list(data)
            ]

            return result

        if self.fkr_list:
            fkr_wirter = PlpFkrCreator()
            fkr_wirter.create(db_records=prepare_record_fkr(self.fkr_list), unload_dir=self.unload_dir)

    def run(self):
        self.statusbar_max_info()
        self.step_info(DATABASE_CONNECTION)
        connection = DatabaseConnection(
            self.login,
            self.password,
            self.database_path,
        )

        self.step_info(CREATE_MAIN)
        self.create_main(connection)

        self.step_info(CREATE_FKR)
        self.create_kfr()

        self.step_info(CREATE_ORG)
        self.create_org(connection)

        self.step_info(CREATE_ZIP)
        return self.zip_files()


class PbsUnload(UnloadAbs):
    """Cметные назначения."""

    prefix = 'pbs'
    dbf_files_names = (PbsMainCreator.file_name, PbsFkrCreator.file_name)

    def __init__(self, login, password, unload_dir, database_path, date_begin, date_end, sql_filter):
        super().__init__(login, password, unload_dir, database_path, date_begin, date_end, sql_filter)
        self.fkr_list = None

    def prepare_sql(self, blank, addition):
        result = blank.format(
            self.date_begin.toString(DATABASE_DATE_FORMAT),
            self.date_end.toString(DATABASE_DATE_FORMAT),
        )
        if addition:
            result += addition

        if self.filter:
            result += PBS_ACCOUNT_FILTER.format(self.filter)

        return result

    def create_main(self, connection):
        outgoing_request = self.prepare_sql(
            PBS_SQL,
            PBS_CONFIG,
        )
        db_records = connection.execute(outgoing_request)
        main_creator = PbsMainCreator()
        main_creator.create(db_records, unload_dir=self.unload_dir)

        self.fkr_list = main_creator.fkr_list

    def create_fkr(self):
        def prepare_record_fkr(data):
            result = [
                dict(zip(FKR_KEYS, fkr_item)) for fkr_item in list(data)
            ]

            return result

        fkr_wirter = PbsFkrCreator()
        fkr_wirter.create(db_records=prepare_record_fkr(self.fkr_list), unload_dir=self.unload_dir)

    def run(self):
        self.statusbar_max_info()
        self.step_info(DATABASE_CONNECTION)
        connection = DatabaseConnection(
            self.login,
            self.password,
            self.database_path,
        )

        self.step_info(CREATE_MAIN)
        self.create_main(connection)

        self.step_info(CREATE_FKR)
        self.create_fkr()

        self.step_info(CREATE_ZIP)
        return self.zip_files()


class ArgUnload(UnloadAbs):
    """Реестр обязательств."""

    prefix = 'arg'
    dbf_files_names = (
        ArgMainCreator.file_name,
        ArgOrgCreator.file_name,
        ArgEstCreator.file_name,
        ArgFkrCreator.file_name,
    )

    def __init__(self, login, password, unload_dir, database_path, date_begin, date_end, sql_filter):
        super().__init__(login, password, unload_dir, database_path, date_begin, date_end, sql_filter)
        self.organizations_ids = self.fkr_list = None

    def prepare_sql(self, blank, addition):
        result = blank.format(
            self.date_begin.toString(DATABASE_DATE_FORMAT),
            self.date_end.toString(DATABASE_DATE_FORMAT),
        )
        if addition:
            result += addition

        if self.filter:
            result += ARG_ACCOUNT_FILTER.format(self.filter)

        return result

    def create_main(self, connection):
        request = self.prepare_sql(
            ARG_BANK_SQL,
            ARG_CONFIG,
        )
        db_records = connection.execute(request)
        request = self.prepare_sql(
            ARG_ORG_SQL,
            ARG_CONFIG,
        )
        db_records.extend(connection.execute(request))

        if db_records:
            main_creator = ArgMainCreator()
            main_creator.create(db_records, unload_dir=self.unload_dir)
            main_creator.create(db_records, create_new_file=False, unload_dir=self.unload_dir)

            self.fkr_list = main_creator.fkr_list
            self.organizations_ids = main_creator.organizations_ids

    def create_org(self, connection):
        request = ORG_INFO_SQL.format(tuple(self.organizations_ids))
        db_records = connection.execute(request)
        ArgOrgCreator().create(db_records, unload_dir=self.unload_dir)

    def create_fkr(self):
        def prepare_record_fkr(data):
            result = [
                dict(zip(FKR_KEYS, fkr_item)) for fkr_item in list(data)
            ]

            return result

        fkr_wirter = ArgFkrCreator()
        fkr_wirter.create(db_records=prepare_record_fkr(self.fkr_list), unload_dir=self.unload_dir)

    def create_est(self, connection):
        request = self.prepare_sql(
            ARG_EST_SQL,
            ARG_CONFIG,
        )
        db_records = connection.execute(request)
        ArgEstCreator().create(db_records, unload_dir=self.unload_dir)

    def run(self):
        self.statusbar_max_info()
        self.step_info(DATABASE_CONNECTION)
        connection = DatabaseConnection(
            self.login,
            self.password,
            self.database_path,
        )

        self.step_info(CREATE_MAIN)
        self.create_main(connection)

        self.step_info(CREATE_ORG)
        self.create_org(connection)

        self.step_info(CREATE_FKR)
        self.create_fkr()

        self.step_info(CREATE_EST)
        self.create_est(connection)

        self.step_info(CREATE_ZIP)
        return self.zip_files()


class MainWindow(QtWidgets.QMainWindow):
    """Класс основного окна.

    Из важного содержит метод unload, отвечающий за выгрузку в dbf.
    """
    UNLOAD_COMBOBOX_DATA = {
        'Платёжные поручения': PlpUnload,
        'Сметные назначения': PbsUnload,
        'Реестр обязательств': ArgUnload,
        # 'Прочие финансовые документы': BndUnload,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        uic.loadUi('krista.ui', self)
        self.threadpool = QThreadPool()
        self.statusbar_progress_bar = QProgressBar()
        self.statusbar.addPermanentWidget(self.statusbar_progress_bar)
        self.database_path_tool_button.clicked.connect(self.select_database_path_dialog)
        self.unload_dir_tool_button.clicked.connect(self.select_unload_dir_dialog)
        self.unload_push_button.clicked.connect(self.unload)
        self.set_default_status()

    def fill_by_config(self, login, password, database_path, unload_dir, filter_, date_begin, date_end):
        self.login_line_edit.setText(login)
        self.password_line_edit.setText(password)
        self.database_path_line_edit.setText(database_path)
        self.unload_dir_line_edit.setText(unload_dir)
        self.filter_line_edit.setText(filter_)
        self.date_begin_date_edit.setDate(datetime.strptime(date_begin, '%d.%m.%Y'))
        self.date_end_date_edit.setDate(datetime.strptime(date_end, '%d.%m.%Y'))

    def select_database_path_dialog(self):
        fname, _ = QFileDialog.getOpenFileName(self, 'Путь к базе данных', os.getcwd(), filter='*.gdb')
        self.database_path_line_edit.setText(fname)

    def select_unload_dir_dialog(self):
        fname = QFileDialog.getExistingDirectory(self, 'Директория для выгрузки', os.getcwd())
        self.unload_dir_line_edit.setText(fname)

    def status_bar_showmessage(self, message):
        self.statusbar.showMessage(message)

    def set_statusbar_text(self, args):
        message, step = args
        self.status_bar_showmessage(message)
        self.statusbar_progress_bar.setValue(self.statusbar_progress_bar.value() + step)

    def set_default_status(self):
        self.statusbar_progress_bar.setMaximum(1)
        self.statusbar_progress_bar.setValue(0)
        self.unload_push_button.setEnabled(True)

    def show_error_message(self, error_info_tuple):
        error_type, error_text = error_info_tuple
        if error_type == DatabaseError:
            QMessageBox.critical(
                self,
                'Соединение с базой данных',
                (
                    f'Ошибка соединения с базой данных {self.database_path_line_edit.text()}'
                )
            )
        elif error_type == FileNotFoundError:
            QMessageBox.critical(
                self,
                'Ошибка создания архива',
                (
                    f'Проверьте доступность пути выгрузки {self.unload_dir_line_edit.text()}'
                )
            )

    def unload(self):
        unload_object_class = self.UNLOAD_COMBOBOX_DATA[self.unload_combobox.currentText()]
        self.unload_push_button.setEnabled(False)
        unload_object = WorkerWrapper(
            unload_object_class(
                self.login_line_edit.text(),
                self.password_line_edit.text(),
                self.unload_dir_line_edit.text(),
                self.database_path_line_edit.text(),
                self.date_begin_date_edit.date(),
                self.date_end_date_edit.date(),
                self.filter_line_edit.text(),
            )
        )
        unload_object.signals.set_progress_max.connect(self.statusbar_progress_bar.setMaximum)
        unload_object.signals.progress.connect(self.set_statusbar_text)
        unload_object.signals.error.connect(self.show_error_message)
        unload_object.signals.result.connect(self.status_bar_showmessage)
        unload_object.signals.finished.connect(self.set_default_status)
        self.threadpool.start(unload_object)


def main():
    app = QApplication(sys.argv)
    config_file = DynamicConfigFile()
    config_file.read()

    window = MainWindow()
    window.fill_by_config(
        config_file.login,
        config_file.password,
        config_file.database_path,
        config_file.unload_dir,
        config_file.filter,
        config_file.date_begin,
        config_file.date_end,
    )
    window.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
