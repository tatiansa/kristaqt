DATE_FORMAT = 'dd.MM.yyyy'
DATABASE_DATE_FORMAT = 'ddMMyyyy'

# платежные поручения
INCOMING_SQL_ADDITION = ' and facialfincaption.progindex in (61, 62, 63, 66) and facialfincaption.buhpaymentcls in (8, 9, 101, 104, 105)'
OUTGOING_SQL_ADDITION = ' and facialfincaption.progindex in (61, 62, 63, 66) and facialfincaption.buhpaymentcls in (7, 16, 100, 114, 115)'

# сметные назначения
# a - budnotify
PBS_CONFIG = ' and budnotify.progindex in (32, 262)'

# реестр обязательств (договоров)
# a - agreements
ARG_CONFIG = ' AND agreements.progindex in (304, 314)'

# прочие финансовые документы
# a - quotestitle
# b - incomes32
bnd_config = 'a.progindex=45 and b.doctype=1010'

FKR_KEYS = ('ID', 'GRBS', 'DIVSN', 'TARGT', 'TARST')
