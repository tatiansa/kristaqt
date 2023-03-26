import os
from abc import ABCMeta
from decimal import Decimal

from dbfpy3 import dbf


class FireBirdGetterMethods:
    """Класс для методов получения и конвертации полей из бд."""

    @staticmethod
    def date_from_double(value):
        return f'{value:.0f}' if value else ''

    @staticmethod
    def to_string(value):
        return f'{value}' if value else ''

    @staticmethod
    def number(value):
        return value if value else 0

    @staticmethod
    def number_prescision2(value):
        return round(Decimal(value), 2) if value else round(Decimal(0), 2)

    @staticmethod
    def number_prescision4(value):
        return round(Decimal(value), 4) if value else round(Decimal(0), 4)

    @staticmethod
    def string_from_float(value):
        return FireBirdGetterMethods.date_from_double(value)

    @staticmethod
    def get_inn(value):
        value = FireBirdGetterMethods.date_from_double(value)
        if len(value) == 9 or len(value) == 11:
            # я не знаю зачем это нужно, аналитик тоже, но раз было в первоисточнике пусть остаётся
            value = f'0{value}'

        return value


class DbfCreatorABS(metaclass=ABCMeta):
    """Базовый класс для записи dbf файла."""

    file_name = None
    dbf_schema_and_getter_map = {}

    def additional_handler(self, dbf_record, firebird_record):
        pass

    def force_encode(self, value):
        """Представляем строки в виде bytes

        :param value: значение
        :return: bytes
        """

        result = value
        if isinstance(value, (str, )):
            result = value.encode()

        return result

    def fkr_handler(self, dbf_record, firebird_record):
        """Заполенение и чтение fkr

        :param dbf_record: запись для dbf
        :param firebird_record: запись из бд
        :return: список кодов
        """

        grbs = FireBirdGetterMethods.to_string(firebird_record['GRBS']).ljust(3, '0')
        divsn = FireBirdGetterMethods.to_string(firebird_record['DIVSN']).ljust(4, '0')
        targt = FireBirdGetterMethods.to_string(firebird_record['TARGT']).ljust(7, '0')
        tarst = FireBirdGetterMethods.to_string(firebird_record['TARST']).ljust(3, '0')
        fkrid = f'{grbs}.{divsn}.{targt}.{tarst}'

        return fkrid, grbs, divsn, targt, tarst

    def create(self, db_records, unload_dir, create_new_file=True):
        with dbf.Dbf(unload_dir + os.sep + self.file_name, new=create_new_file, code_page='cp866') as dbf_db:
            if create_new_file:
                dbf_db.add_field(*self.dbf_schema_and_getter_map.keys())

            for firebird_db_record in db_records:
                dbf_db_record = dbf_db.new()
                for key, getter in self.dbf_schema_and_getter_map.items():
                    _, column, _ = key
                    if isinstance(getter, (tuple, list)):
                        getter, fire_bird_column = getter
                        dbf_db_record[column.upper()] = self.force_encode(getter(firebird_db_record[fire_bird_column]))
                    elif getter and callable(getter):
                        dbf_db_record[column.upper()] = self.force_encode(getter(firebird_db_record[column]))

                self.additional_handler(dbf_db_record, firebird_db_record)
                dbf_db.write(dbf_db_record)


class PlpMainCreator(DbfCreatorABS):
    file_name = 'plp_main.dbf'
    dbf_schema_and_getter_map = {
        ("C", 'ID', 15): FireBirdGetterMethods.to_string,
        ("C", 'ENT_INN', 12): FireBirdGetterMethods.string_from_float,
        ("C", 'ENT_KPP', 9): FireBirdGetterMethods.to_string,
        ("C", 'ENT_SNAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'ENT_NAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'ENT_LS', 25): FireBirdGetterMethods.to_string,
        ("C", 'ENT_MFO', 9): FireBirdGetterMethods.to_string,
        ("C", 'ENT_COR', 20): FireBirdGetterMethods.to_string,
        ("C", 'ENT_RS', 20): FireBirdGetterMethods.to_string,
        ("C", 'DOCNUMBER', 50): FireBirdGetterMethods.to_string,
        ("C", 'DOCDATE', 8): (FireBirdGetterMethods.date_from_double, 'DOCUMENTDATE'),
        ("N", 'CREDIT', 15): FireBirdGetterMethods.number,
        ("C", 'PAYDATE', 8): FireBirdGetterMethods.date_from_double,
        ("C", 'ACCEPTDATE', 8): FireBirdGetterMethods.date_from_double,
        ("C", 'KAZN_LS', 20): FireBirdGetterMethods.to_string,
        ("C", 'NOTE', 255): FireBirdGetterMethods.to_string,
        ("C", 'DEST_ORG', 15): FireBirdGetterMethods.to_string,
        ("C", 'DEST_RS', 20): FireBirdGetterMethods.to_string,
        ("C", 'DEST_MFO', 9): FireBirdGetterMethods.to_string,
        ("C", 'DEST_COR', 20): FireBirdGetterMethods.to_string,
        ("C", 'NDS', 100): FireBirdGetterMethods.to_string,
        ("C", 'TAXNOTE', 150): FireBirdGetterMethods.to_string,
        ("C", 'FKRID', 30): None,
        ("C", 'SOURCEKESR', 10): FireBirdGetterMethods.to_string,
        ("C", 'REFBU', 5): FireBirdGetterMethods.to_string,
        ("C", 'AGRID', 15): FireBirdGetterMethods.to_string,
        ("N", 'BUHPAYCLS', 2): FireBirdGetterMethods.number,
    }

    def __init__(self):
        self.organizations_ids = set()
        self.fkr_list = set()

    def additional_handler(self, dbf_record, firebird_record):
        fkrid, grbs, divsn, targt, tarst = self.fkr_handler(dbf_record, firebird_record)
        dbf_record['FKRID'] = fkrid
        self.fkr_list.add((fkrid, grbs, divsn, targt, tarst))
        self.organizations_ids.add(firebird_record['DEST_ORG'])


class PlpOrgCreator(DbfCreatorABS):
    file_name = 'plp_org.dbf'
    dbf_schema_and_getter_map = {
        ("C", 'ID', 15): FireBirdGetterMethods.to_string,
        ("C", 'INN', 12): FireBirdGetterMethods.get_inn,
        ("C", 'KPP', 9): FireBirdGetterMethods.to_string,
        ("C", 'SHORTNAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'NAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'OKATO', 11): FireBirdGetterMethods.to_string,
    }


class PlpFkrCreator(DbfCreatorABS):
    file_name = 'plp_fkr.dbf'
    dbf_schema_and_getter_map = {
        ("C", 'ID', 30): FireBirdGetterMethods.to_string,
        ("C", 'GRBS', 3): FireBirdGetterMethods.to_string,
        ("C", 'DIVSN', 4): FireBirdGetterMethods.to_string,
        ("C", 'TARGT', 7): FireBirdGetterMethods.to_string,
        ("C", 'TARST', 3): FireBirdGetterMethods.to_string,
    }


class PbsMainCreator(DbfCreatorABS):
    file_name = 'pbs_main.dbf'
    dbf_schema_and_getter_map = {
        ("C", 'ID', 15): FireBirdGetterMethods.to_string,
        ("C", 'ID_BUDGETD', 15): FireBirdGetterMethods.to_string,
        ("C", 'ENT_INN', 12): FireBirdGetterMethods.string_from_float,
        ("C", 'ENT_KPP', 9): FireBirdGetterMethods.to_string,
        ("C", 'ENT_SNAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'ENT_NAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'ENT_LS', 25): FireBirdGetterMethods.to_string,
        ("C", 'DAT', 8): FireBirdGetterMethods.date_from_double,
        ("C", 'ANUMBER', 20): FireBirdGetterMethods.to_string,
        ("C", 'DOCDAT', 8): FireBirdGetterMethods.date_from_double,
        ("C", 'DOCNUMBER', 50): FireBirdGetterMethods.to_string,
        ("C", 'NOTE', 255): FireBirdGetterMethods.to_string,
        ("C", 'RASP_NAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'FKRID', 30): None,
        ("C", 'KOSGU', 10): FireBirdGetterMethods.to_string,
        ("C", 'SUMM', 15): FireBirdGetterMethods.to_string,
        ("C", 'KVD', 15): (FireBirdGetterMethods.string_from_float, 'MEANSTYPE'),
    }

    def __init__(self):
        self.fkr_list = set()

    def additional_handler(self, dbf_record, firebird_record):
        fkrid, grbs, divsn, targt, tarst = self.fkr_handler(dbf_record, firebird_record)
        dbf_record['FKRID'] = fkrid
        self.fkr_list.add((fkrid, grbs, divsn, targt, tarst))


class PbsFkrCreator(PlpFkrCreator):
    file_name = 'pbs_fkr.dbf'


class ArgMainCreator(DbfCreatorABS):
    file_name = 'arg_main.dbf'
    dbf_schema_and_getter_map = {
        ("C", 'ID', 15): FireBirdGetterMethods.to_string,
        ("C", 'PARID', 15): FireBirdGetterMethods.to_string,
        ("C", 'ENT_INN', 12): FireBirdGetterMethods.string_from_float,
        ("C", 'ENT_KPP', 9): FireBirdGetterMethods.to_string,
        ("C", 'ENT_SNAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'ENT_NAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'AGRTYPE', 1): (FireBirdGetterMethods.to_string, 'AGREEMENTTYPE'),
        ("C", 'DOCNUMBER', 50): FireBirdGetterMethods.to_string,
        ("C", 'AGRDATE', 8): (FireBirdGetterMethods.date_from_double, 'AGREEMENTDATE'),
        ("C", 'AGRBEGDATE', 8): (FireBirdGetterMethods.date_from_double, 'AGREEMENTBEGINDATE'),
        ("C", 'AGRENDDATE', 8): (FireBirdGetterMethods.date_from_double, 'AGREEMENTENDDATE'),
        ("C", 'ADJDOCNUM', 50): (FireBirdGetterMethods.to_string, 'ADJUSTMENTDOCNUMBER'),
        ("C", 'REESTRNUM', 20): (FireBirdGetterMethods.to_string, 'REESTRNUMBER'),
        ("C", 'EXECUTER', 15): (FireBirdGetterMethods.to_string, 'EXECUTER_REF'),
        ("C", 'EX_RS', 20): FireBirdGetterMethods.to_string,
        ("C", 'EX_MFO', 9): FireBirdGetterMethods.to_string,
        ("C", 'EX_COR', 20): FireBirdGetterMethods.to_string,
        ("C", 'FKR', 30): None,
        ("C", 'ACCEPTDATE', 8): FireBirdGetterMethods.date_from_double,
        ("C", 'KOSGU', 8): FireBirdGetterMethods.to_string,
        ("N", 'MONTH01', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH02', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH03', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH04', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH05', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH06', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH07', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH08', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH09', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH10', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH11', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'MONTH12', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'SUMM', 15): (FireBirdGetterMethods.number_prescision2, 'AGREEMENTSUMMA'),
        ("C", 'REFBU', 5): (FireBirdGetterMethods.to_string, 'MEANSTYPE'),
        ("C", 'PURPORTDOC', 255): FireBirdGetterMethods.to_string,
        ("C", 'DOCINDEX', 3): (FireBirdGetterMethods.to_string, 'PROGINDEX'),
    }

    def __init__(self):
        self.organizations_ids = set()
        self.fkr_list = set()

    def additional_handler(self, dbf_record, firebird_record):
        fkrid, grbs, divsn, targt, tarst = self.fkr_handler(dbf_record, firebird_record)
        dbf_record['FKR'] = fkrid
        self.fkr_list.add((fkrid, grbs, divsn, targt, tarst))
        self.organizations_ids.add(firebird_record['EXECUTER_REF'])


class ArgEstCreator(DbfCreatorABS):
    file_name = 'arg_est.dbf'
    dbf_schema_and_getter_map = {
        ("C", 'ID', 20): (FireBirdGetterMethods.to_string, 'EST_ID'),
        ("C", 'RECORDIDX', 20): (FireBirdGetterMethods.to_string, 'ARG_ID'),
        ("N", 'AMOUNT', 17): FireBirdGetterMethods.number_prescision4,
        ("N", 'SUMMA', 15): FireBirdGetterMethods.number_prescision2,
        ("C", 'NAME', 255): (FireBirdGetterMethods.number, 'TDO_NAME'),
        ("C", 'OKDP_CODE', 20): (FireBirdGetterMethods.to_string, 'SOURCECODE'),
        ("C", 'OKPD2_CODE', 20): FireBirdGetterMethods.to_string,
        ("C", 'MSM_ID', 20): FireBirdGetterMethods.to_string,
        ("C", 'MSM_NAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'MSM_SHORTN', 50): (FireBirdGetterMethods.to_string, 'MSM_SHORTNAME'),
    }


class ArgOrgCreator(PlpOrgCreator):
    file_name = 'arg_org.dbf'


class ArgFkrCreator(PlpFkrCreator):
    file_name = 'arg_fkr.dbf'


class BndMainCreator(DbfCreatorABS):
    file_name = 'bnd_main.dbf'
    dbf_schema_and_getter_map = {
        ("C", 'ID', 15): FireBirdGetterMethods.to_string,
        ("C", 'ENT_INN', 12): FireBirdGetterMethods.to_string,
        ("C", 'ENT_KPP', 9): FireBirdGetterMethods.to_string,
        ("C", 'ENT_SNAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'ENT_NAME', 255): FireBirdGetterMethods.to_string,
        ("C", 'ENT_LS', 25): FireBirdGetterMethods.to_string,
        ("C", 'K_ID', 15): FireBirdGetterMethods.to_string,
        ("C", 'K_RS', 20): FireBirdGetterMethods.to_string,
        ("C", 'K_MFO', 9): FireBirdGetterMethods.to_string,
        ("C", 'K_COR', 20): FireBirdGetterMethods.to_string,
        ("C", 'DOCNUM', 50): FireBirdGetterMethods.to_string,
        ("C", 'DOCDATE', 8): FireBirdGetterMethods.date_from_double,
        ("C", 'ACCEPTDATE', 8): FireBirdGetterMethods.date_from_double,
        ("N", 'CREDIT', 15): FireBirdGetterMethods.number_prescision2,
        ("N", 'DEBIT', 15): FireBirdGetterMethods.number_prescision2,
        ("C", 'NOTE', 255): FireBirdGetterMethods.to_string,
        ("C", 'CLSTYPE', 1): FireBirdGetterMethods.to_string,
        ("C", 'KD', 20): (FireBirdGetterMethods.to_string, 'KDVALUE'),
        ("C", 'IFS', 20): (FireBirdGetterMethods.to_string, 'FINSOURCEVALUE'),
        ("C", 'KVD', 20): (FireBirdGetterMethods.to_string, 'MEANSTYPE'),
    }
    
    def __init__(self):
        self.organizations_ids = set()
        self.fkr_list = set()
    
    def additional_handler(self, dbf_record, firebird_record):
        fkrid, grbs, divsn, targt, tarst = self.fkr_handler(dbf_record, firebird_record)
        dbf_record['FKRID'] = fkrid
        self.fkr_list.add((fkrid, grbs, divsn, targt, tarst))
        self.organizations_ids.add(firebird_record['DEST_ORG'])


class BndOrgCreator(PlpOrgCreator):
    file_name = 'bnd_org.dbf'


class BndFkrCreator(PlpFkrCreator):
    file_name = 'bnd_fkr.dbf'