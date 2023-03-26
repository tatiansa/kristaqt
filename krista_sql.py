# исходящие платежи
# TODO: запросить у пользователя структуру базы и сделать нормальную фильтрацию по коду счета
PLP_ACCOUNT_FILTER = ' and facialfincaption.destfacialacc_cls = {}'
PLP_IN_SQL = """with acc_service_ref_info AS (
    select ORG_ACCOUNTS.ID, ORG_ACCOUNTS.ACC, BANKS.MFO, BANKS.COR from ORG_ACCOUNTS 
        join BANKS on (BANKS.ID = ORG_ACCOUNTS.BANK_REF)
    ),
aggreements_step_info AS (
    select agreementsteps.RECORDINDEX, FACIALFINDETAIL.ID from FACIALFINDETAIL 
        join PAYMENTSCHEDULE on (FACIALFINDETAIL.sourcepromise = PAYMENTSCHEDULE.ANUMBER)
        join agreementsteps on (PAYMENTSCHEDULE.RECORDINDEX = AGREEMENTSTEPS.ID)
    ),
ORGANIZATIONS_INFO AS (
    SELECT facialacc_cls.ID, ORGANIZATIONS.INN, ORGANIZATIONS.NAME, ORGANIZATIONS.SHORTNAME, ORGANIZATIONS.INN20 FROM facialacc_cls 
        JOIN ORGANIZATIONS ON (facialacc_cls.ORG_REF = ORGANIZATIONS.ID)
    )
select 
    facialfincaption.id, 
    facialfincaption.docnumber, 
    facialfincaption.documentdate, 
    facialfincaption.paydate, 
    facialfincaption.acceptdate, 
    facialfincaption.note, 
    facialfincaption.nds, 
    facialfincaption.credit, 
    facialfincaption.destfacialacc_cls as ent_ls, 
    facialfincaption.taxnote,  
    FACIALFINDETAIL.sourcekfsr as divsn, 
    KESR.code as sourcekesr, 
    FACIALFINDETAIL.sourcemeanstype as refbu, 
    DEST_ACCOUNT.org_ref as dest_org, 
    DEST_ACCOUNT.bank_ref, 
    DEST_ACCOUNT.acc as dest_rs, 
    DEST_BANK.mfo as dest_mfo, 
    DEST_BANK.cor as dest_cor, 
    KVSR.code as grbs, 
    kcsr.code as targt, 
    KVR.code as tarst, 
    ORGANIZATIONS_.inn as ent_inn, 
    ORGANIZATIONS_.name as ent_name, 
    ORGANIZATIONS_.shortname as ent_sname, 
    ORGANIZATIONS_.inn20 as ent_kpp, 
    SOURCE_ACCOUNT.service_acc_ref, 
    case 
        when SOURCE_ACCOUNT.service_acc_ref is null then null
        else SOURCE_ACCOUNT.acc
    end as kazn_ls,
    case 
        when SOURCE_ACCOUNT.service_acc_ref is null then SOURCE_ACCOUNT.acc
        else acc_service_ref_info.acc
    end as ent_rs,
    case 
        when SOURCE_ACCOUNT.service_acc_ref is null then SOURCE_BANK.mfo
        else acc_service_ref_info.MFO
    end as ent_mfo,
    case 
        when SOURCE_ACCOUNT.service_acc_ref is null then SOURCE_BANK.cor
        else acc_service_ref_info.COR
    end as ent_cor,
    aggreements_step_info.RECORDINDEX as agrid,
    0 as buhpaycls
from FACIALFINCAPTION 
    join FACIALFINDETAIL on (FACIALFINCAPTION.ID = FACIALFINDETAIL.RECORDINDEX)
    /*DEST*/
    join ORG_ACCOUNTS DEST_ACCOUNT on (FACIALFINCAPTION.sourceaccount = DEST_ACCOUNT.ID)
    join BANKS DEST_BANK on (DEST_BANK.ID = DEST_ACCOUNT.BANK_REF)
    /**/
    /*SOURCE*/
    join ORG_ACCOUNTS SOURCE_ACCOUNT on (FACIALFINCAPTION.destaccount = SOURCE_ACCOUNT.ID)
    join BANKS SOURCE_BANK on (SOURCE_ACCOUNT.BANK_REF = SOURCE_BANK.ID)
    /**/
    /*ORGANIZATIONS*/
    JOIN ORGANIZATIONS_INFO ORGANIZATIONS_ ON (facialfincaption.destfacialacc_cls = ORGANIZATIONS_.ID)
    /**/
    left join KESR on (FACIALFINDETAIL.SOURCEKESR = KESR.ID)
    left join KVSR on (FACIALFINDETAIL.SOURCEKVSR = KVSR.ID )
    left join kcsr on (FACIALFINDETAIL.SOURCEkcsr = kcsr.ID )
    left join KVR on (FACIALFINDETAIL.SOURCEKVSR = KVR.ID )
    left join acc_service_ref_info on (SOURCE_ACCOUNT.SERVICE_ACC_REF = acc_service_ref_info.ID)
    left join aggreements_step_info on (FACIALFINDETAIL.ID = aggreements_step_info.ID)
where 
    facialfincaption.reject_cls is null and 
    facialfincaption.acceptdate>='{}' and facialfincaption.acceptdate<='{}'"""

PLP_OUT_SQL = """with acc_service_ref_info AS (
    select ORG_ACCOUNTS.ID, ORG_ACCOUNTS.ACC, BANKS.MFO, BANKS.COR from ORG_ACCOUNTS 
        join BANKS on (BANKS.ID = ORG_ACCOUNTS.BANK_REF)
    ),
aggreements_step_info AS (
    select agreementsteps.RECORDINDEX, FACIALFINDETAIL.ID from FACIALFINDETAIL 
        join PAYMENTSCHEDULE on (FACIALFINDETAIL.sourcepromise = PAYMENTSCHEDULE.ANUMBER)
        join agreementsteps on (PAYMENTSCHEDULE.RECORDINDEX = AGREEMENTSTEPS.ID)
    ),
ORGANIZATIONS_INFO AS (
    SELECT facialacc_cls.ID, ORGANIZATIONS.INN, ORGANIZATIONS.NAME, ORGANIZATIONS.SHORTNAME, ORGANIZATIONS.INN20 FROM facialacc_cls 
        JOIN ORGANIZATIONS ON (facialacc_cls.ORG_REF = ORGANIZATIONS.ID)
    )
select 
    facialfincaption.id, 
    facialfincaption.docnumber, 
    facialfincaption.documentdate, 
    facialfincaption.paydate, 
    facialfincaption.acceptdate, 
    facialfincaption.note, 
    facialfincaption.nds, 
    facialfincaption.credit, 
    facialfincaption.sourcefacialacc_cls as ent_ls, 
    facialfincaption.taxnote,  
    FACIALFINDETAIL.sourcekfsr as divsn, 
    KESR.code as sourcekesr, 
    FACIALFINDETAIL.sourcemeanstype as refbu, 
    DEST_ACCOUNT.org_ref as dest_org, 
    DEST_ACCOUNT.bank_ref, 
    DEST_ACCOUNT.acc as dest_rs, 
    DEST_BANK.mfo as dest_mfo, 
    DEST_BANK.cor as dest_cor, 
    KVSR.code as grbs, 
    kcsr.code as targt, 
    KVR.code as tarst, 
    ORGANIZATIONS_.inn as ent_inn, 
    ORGANIZATIONS_.name as ent_name, 
    ORGANIZATIONS_.shortname as ent_sname, 
    ORGANIZATIONS_.inn20 as ent_kpp, 
    SOURCE_ACCOUNT.service_acc_ref, 
    case 
        when SOURCE_ACCOUNT.service_acc_ref is null then null
        else SOURCE_ACCOUNT.acc
    end as kazn_ls,
    case 
        when SOURCE_ACCOUNT.service_acc_ref is null then SOURCE_ACCOUNT.acc
        else acc_service_ref_info.acc
    end as ent_rs,
    case 
        when SOURCE_ACCOUNT.service_acc_ref is null then SOURCE_BANK.mfo
        else acc_service_ref_info.MFO
    end as ent_mfo,
    case 
        when SOURCE_ACCOUNT.service_acc_ref is null then SOURCE_BANK.cor
        else acc_service_ref_info.COR
    end as ent_cor,
    aggreements_step_info.RECORDINDEX as agrid,
    1 as buhpaycls
from FACIALFINCAPTION 
    join FACIALFINDETAIL on (FACIALFINCAPTION.ID = FACIALFINDETAIL.RECORDINDEX)
    /*DEST*/
    join ORG_ACCOUNTS DEST_ACCOUNT on (FACIALFINCAPTION.DESTACCOUNT = DEST_ACCOUNT.ID)
    join BANKS DEST_BANK on (DEST_BANK.ID = DEST_ACCOUNT.BANK_REF)
    /**/
    /*SOURCE*/
    join ORG_ACCOUNTS SOURCE_ACCOUNT on (FACIALFINCAPTION.SOURCEACCOUNT = SOURCE_ACCOUNT.ID)
    join BANKS SOURCE_BANK on (SOURCE_ACCOUNT.BANK_REF = SOURCE_BANK.ID)
    /**/
    /*ORGANIZATIONS*/
    JOIN ORGANIZATIONS_INFO ORGANIZATIONS_ ON (facialfincaption.SOURCEFACIALACC_CLS = ORGANIZATIONS_.ID)
    /**/
    left join KESR on (FACIALFINDETAIL.SOURCEKESR = KESR.ID)
    left join KVSR on (FACIALFINDETAIL.SOURCEKVSR = KVSR.ID )
    left join kcsr on (FACIALFINDETAIL.SOURCEkcsr = kcsr.ID )
    left join KVR on (FACIALFINDETAIL.SOURCEKVSR = KVR.ID )
    left join acc_service_ref_info on (SOURCE_ACCOUNT.SERVICE_ACC_REF = acc_service_ref_info.ID)
    left join aggreements_step_info on (FACIALFINDETAIL.ID = aggreements_step_info.ID)
where 
    facialfincaption.reject_cls is null and 
    facialfincaption.acceptdate>='{}' and facialfincaption.acceptdate<='{}'"""


# TODO: запросить у пользователя структуру базы и сделать нормальную фильтрацию по коду счета
PBS_ACCOUNT_FILTER = ' and budgetdata.facialacc_cls = {}'

# запрос по сметным назначениям
PBS_SQL = """select 
    budnotify.id,
    budgetdata.ID as ID_BUDGETD,
    budnotify.dat, 
    budnotify.anumber, 
    budnotify.docdat, 
    budnotify.docnumber, 
    budnotify.note, 
    budnotify.facialacccls as ent_ls, 
    budnotify.orgref as rasp_id, 
    budgetdata.kfsr as divsn, 
    kesr.code as kosgu, 
    budgetdata.summayear1 as summ, 
    budgetdata.org_ref as ent_id, 
    budgetdata.meanstype as meanstype, 
    notify_organizations.name as rasp_name, 
    data_organizations.name as ent_name, 
    data_organizations.inn as ent_inn, 
    data_organizations.shortname as ent_sname, 
    data_organizations.inn20 as ent_kpp, 
    kvsr.code as grbs, 
    kcsr.code as targt, 
    kvr.code as tarst 
from budgetdata
    join budnotify  on (budnotify.id = budgetdata.recordindex)
    join organizations notify_organizations on (budnotify.ORGREF = notify_organizations.id)
    join organizations data_organizations on (budgetdata.org_ref = data_organizations.id)
    left join kvsr on (budgetdata.kvsr = kvsr.id)
    left join kcsr on (budgetdata.kcsr = kcsr.id)
    left join kvr on (budgetdata.kvr = kvr.id)
    left join kesr on (budgetdata.kesr = KESR.ID)
where 
    budnotify.rejectnote is null and 
    budnotify.rejectcls is null  and 
    budnotify.dat>='{}' and budnotify.dat<='{}'"""

ARG_ACCOUNT_FILTER = ' and a.facialacc_cls={}'
ARG_BANK_SQL = """select 
    agreements.id, 
    agreements.agreementtype, 
    agreements.docnumber, 
    agreements.agreementdate, 
    agreements.agreementbegindate, 
    agreements.agreementenddate, 
    agreements.executer_ref, 
    agreements.purportdoc, 
    agreements.progindex, 
    agreements.adjustmentdocnumber, 
    agreements.reestrnumber, 
    agreements.agreementsumma, 
    paymentschedule.acceptdate, 
    paymentschedule.kfsr as divsn, 
    kesr.code as kosgu, 
    paymentschedule.month01, 
    paymentschedule.month02, 
    paymentschedule.month03, 
    paymentschedule.month04, 
    paymentschedule.month05, 
    paymentschedule.month06, 
    paymentschedule.month07, 
    paymentschedule.month08, 
    paymentschedule.month09, 
    paymentschedule.month10, 
    paymentschedule.month11, 
    paymentschedule.month12, 
    case 
        when paymentschedule.parentnumber is null then anumber_join.agreementref
        else paymentschedule.parentnumber
    end as parid,
    paymentschedule.meanstype, 
    paymentschedule.summa, 
    kvsr.code as grbs, 
    kcsr.code as targt, 
    kvr.code as tarst, 
    organizations.inn as ent_inn, 
    organizations.name as ent_name, 
    organizations.shortname as ent_sname, 
    organizations.inn20 as ent_kpp, 
    org_accounts.acc as ex_rs, 
    banks.mfo as ex_mfo, 
    banks.cor as ex_cor 
from agreements 
    join paymentschedule on (agreements.id = paymentschedule.agreementref)
    left join kvsr on (paymentschedule.kvsr = kvsr.id)
    left join kcsr on (paymentschedule.kcsr = kcsr.id)
    left join kvr on (paymentschedule.kvr = kvr.id)
    join organizations on (agreements.client_ref = organizations.id)
    join org_accounts on (agreements.executeraccref = org_accounts.id)
    join banks on (org_accounts.bank_ref = banks.id)
    join kesr on (paymentschedule.kesr = kesr.id)
    left JOIN paymentschedule anumber_join on (anumber_join.ANUMBER = paymentschedule.parentnumber)
where 
    agreements.rejectcause is null and 
    agreements.rejectcls is null and 
    agreements.acceptdate>='{}' and agreements.acceptdate<='{}'"""

ARG_ORG_SQL = """select 
    agreements.id, 
    agreements.agreementtype, 
    agreements.docnumber, 
    agreements.agreementdate, 
    agreements.agreementbegindate, 
    agreements.agreementenddate, 
    agreements.executer_ref, 
    agreements.purportdoc, 
    agreements.progindex, 
    agreements.adjustmentdocnumber, 
    agreements.reestrnumber,
    agreements.agreementsumma, 
    paymentschedule.acceptdate, 
    paymentschedule.kfsr as divsn, 
    kesr.code as kosgu, 
    paymentschedule.month01, 
    paymentschedule.month02, 
    paymentschedule.month03, 
    paymentschedule.month04, 
    paymentschedule.month05, 
    paymentschedule.month06, 
    paymentschedule.month07, 
    paymentschedule.month08, 
    paymentschedule.month09, 
    paymentschedule.month10, 
    paymentschedule.month11, 
    paymentschedule.month12,
    case 
        when paymentschedule.parentnumber is null then anumber_join.agreementref
        else paymentschedule.parentnumber
    end as parid,
    paymentschedule.meanstype, 
    paymentschedule.summa, 
    kvsr.code as grbs, 
    kcsr.code as targt, 
    kvr.code as tarst, 
    organizations.inn as ent_inn, 
    organizations.name as ent_name, 
    organizations.shortname as ent_sname, 
    organizations.inn20 as ent_kpp,
    null as ex_rs, 
    null as ex_mfo, 
    null as ex_cor 
from agreements 
    join paymentschedule on (paymentschedule.agreementref = agreements.id)
    left join kvsr on (paymentschedule.kvsr = kvsr.id)
    left join kcsr on (paymentschedule.kcsr = kcsr.id)
    left join kvr on (paymentschedule.kvr = kvr.id)
    join organizations on (agreements.client_ref = organizations.id)
    join kesr on (paymentschedule.kesr = kesr.id)
    left JOIN paymentschedule anumber_join on (anumber_join.ANUMBER = paymentschedule.parentnumber)
where 
    agreements.rejectcause is null and 
    agreements.rejectcls is null and
    agreements.executeraccref is null and 
    agreements.acceptdate>='{}' and agreements.acceptdate<='{}'"""

ARG_EST_SQL = """select agreements.id as arg_id, 
estimate.id as est_id, 
estimate.amount, 
estimate.summa, 
tenderobjects.name as tdo_name, 
case 
  when okdp.id is null then 0 
  else okdp.sourcecode 
end as sourcecode, 
tenderobjects.okpd2 as okpd2_code, 
measurementcls.id as msm_id, 
measurementcls.name as msm_name, 
measurementcls.shortname as msm_shortname 
from estimate 
    join agreements on agreements.id=estimate.recordindex 
    join tenderobjects on tenderobjects.id=estimate.productcls 
    join measurementcls on measurementcls.id=tenderobjects.measurementcls 
    left outer join okdp on okdp.id=tenderobjects.okdp 
where 
    agreements.rejectcause is null and 
    agreements.rejectcls is null and 
    agreements.acceptdate>='{}' and agreements.acceptdate<='{}'"""

BND_MAIN_SQL = """select 
    quotestitle.acceptdate, 
    incomes32.id, 
    incomes32.docnum, 
    incomes32.docdate, 
    incomes32.org_ref as k_id, 
    incomes32.inn as k_inn, 
    incomes32.note, 
    incomes32.credit, 
    incomes32.debit, 
    incomes32.facialacc_cls as ent_ls, 
    incomes32.clstype, 
    incomes32.meanstype, 
    banks.mfo as k_mfo, 
    banks.cor as k_cor, 
    org_accounts.acc as k_rs, 
    kd.kdvalue, 
    innerfinsource.finsourcevalue, 
    organizations.inn as ent_inn, 
    organizations.inn20 as ent_kpp, 
    organizations.shortname as ent_sname, 
    organizations.name as ent_name 
from quotestitle 
    join incomes32 on (quotestitle.id = incomes32.recordindex) 
    join org_accounts on (incomes32.accountref = org_accounts.id)
    join banks on (org_accounts.bank_ref = banks.id)
    join kd on (incomes32.kd = kd.id)
    join innerfinsource on (innerfinsource.id = incomes32.ifs)
    join facialacc_cls on (facialacc_cls.id = incomes32.facialacc_cls)
    join organizations on (organizations.id = facialacc_cls.org_ref)
where 
    quotestitle.rejectcls is null and 
    quotestitle.acceptdate>='{}' and quotestitle.acceptdate<='{}'"""

ORG_INFO_SQL = """select id, inn, inn20 as kpp, name, shortname, okato from organizations where id in {}"""