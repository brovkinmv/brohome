 
CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3049_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

begin 
-- при сохранении документа Карточка эксперта


update ks_ddlcontrol.f004_doc
set n_expert = kod_tf||right('00000'||(select cast(max(cast(right(coalesce(n_expert,'0'),5) as int))+1 as varchar) from ks_ddlcontrol.f004_doc d1 where d1.f001=d.f001),5)
from ks_ddlcontrol.f004_doc d
inner join ks_ddlcontrol.f001_r s on d.f001=s.id 
inner join ks_ddlcontrol.f010 s1 on s.tf_kod=s1.id
where d.id = v_doc_id and ks_ddlcontrol.f004_doc.id = v_doc_id and ks_ddlcontrol.f004_doc.n_expert is null;
update ks_ddlcontrol.f004_doc 
set date_red = (select cast(to_char(dbo.getWorkDate(), 'YYYYMMDD') as int)) 
where ks_ddlcontrol.f004_doc.id = v_doc_id;
end;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3107_3812_create_new_doc_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- 9.3.	ДЕЙСТВИЕ «Создать исправленную часть» для документа «Файлы реестров счетов по оплате медицинских услуг (R-файл и D-файл –  Исходящий МТР)»

declare
--v_doc_id integer;
v_new_doc_id integer;

v_filename varchar(100);

v_Unit uuid;
v_old_cnt integer;
v_new_cnt integer;

v_num_rzl integer;
v_new_id_zap integer;

v_id_rzl integer;
v_id_sl integer;
v_id_usl integer;
v_id_usl_onk integer;
v_id_usl_onk1 integer;
v_id_usl_onk2 integer;

v_new_id_rzl integer;
v_new_id_sl integer;
v_new_id_usl integer;
v_new_id_usl_onk integer;
v_new_id_usl_onk1 integer;
v_new_id_usl_onk2 integer;

begin 

--v_doc_id := 100;

select dbo.sys_guid() 
into v_Unit;
-- select * from KS_DDLControl.zl_list where left(filename, 1)='D'
--delete from KS_DDLControl.zl_list where left(filename, 1)='D'  and st_owner=92 

v_old_cnt := 0;

create temporary table t_d on commit drop as -- текущий документ
select 
d.status as Статус,
filename as Имя_файла,
data_norm as Дата,
left(cast(data_norm as varchar(8)), 4) as Y,
f10_1.kod_tf as Кто,
f10_2.kod_tf as Кому
from KS_DDLControl.zl_list as d -- Сведения об оказанной МП / Файлы реестров счетов по оплате медицинских услуг (R-файл и D-файл –  Исходящий МТР)

inner join ks_ddlcontrol.o002 o1 ON o1.id = d.c_okato1_o002
inner join ks_ddlcontrol.f010_vers f10v1 ON f10v1.kod_okato = o1.id and coalesce(f10v1.datebeg,0) = (select max(coalesce(f10v1_v.datebeg,0)) from ks_ddlcontrol.f010_vers f10v1_v where (f10v1_v.id_up = f10v1.id_up and coalesce(f10v1_v.datebeg,0) <= d.data_norm))
inner join ks_ddlcontrol.f010 f10_1 ON f10_1.id = f10v1.id_up

inner join ks_ddlcontrol.o002 o2 ON o2.id = d.okato_oms_o002
inner join ks_ddlcontrol.f010_vers f10v2 ON f10v2.kod_okato = o2.id and coalesce(f10v2.datebeg,0) = (select max(coalesce(f10v2_v.datebeg,0)) from ks_ddlcontrol.f010_vers f10v2_v where (f10v2_v.id_up = f10v2.id_up and coalesce(f10v2_v.datebeg,0) <= d.data_norm))
inner join ks_ddlcontrol.f010 f10_2 ON f10_2.id = f10v2.id_up

where d.id = v_doc_id
;
if exists (select * from t_d where Статус in (140, 131, 132) ) then
	RAISE EXCEPTION 'Исправленную часть возможно сделать только для актуальных (отправленных) документов';
--	RAISE notice 'Исправленную часть возможно сделать только для актуальных (отправленных) документов';
end if
;
create temporary table t_inst on commit drop as -- исходные данные
select 
zap.id,
correction as Исправленние,
zap.pacient_spr as ПАЦИЕНТ_ID, --Сведения_о_пациенте
zap.z_sl_spr as ЗСЛ_ID -- Реестр_законченных_случаев
from ks_ddlcontrol.zl_list_zap as zap  -- ТЧ Записи
where id_up = v_doc_id and correction = 1
;
if not exists (select * from t_inst) then
	RAISE EXCEPTION 'В документе отсутсвуют отмеченные записи для формирование испраленной части';
--	RAISE notice 'В документе отсутсвуют отмеченные записи для формирование испраленной части';
end if
;

create temporary table t_inst0 on commit drop as -- исходные данные - для цикла
select *
from t_inst
;

create temporary table v_temp_exd on commit drop as -- существующие документы (4.	ИСПРАВЛЕНИЯ_ЗА_ГОД)
select 
t.id,
filename as Имя_файла,
data_norm as Дата,
cast(left(cast(data_norm as varchar(8)), 4) as int) as Y
from t_d as d
inner join KS_DDLControl.zl_list as t on t.id <> v_doc_id and t.documentid=3812 and left(t.filename, 1) = 'D' and left(cast(t.data_norm as varchar(8)), 4) = d.Y
;
select count(id) into v_old_cnt from v_temp_exd
; 
select 
'D'||d.Кто||d.Кому||right(d.Y, 2)||right('0000'||cast((v_old_cnt+1) as varchar(10)), 4)
into v_filename
from t_d as d
;
RAISE notice 'имя нового файла: %', v_filename;

insert into KS_DDLControl.zl_list (
documentid,
status , -- Статус
st_owner , -- Владелец_статуса
st_date , -- Дата_установки_статуса
data_norm , -- Дата_(нормализованный)
c_okato1_o002 , -- спр._ОКАТО_территории,_выставившей_счет
okato_oms_o002 , -- спр._ОКАТО_территории_страхования_по_ОМС_(территория,_в_которую_выставляется_счет)
filename , -- Имя_файла
year , -- Отчетный_год
month , -- Отчетный_месяц
nschet , -- Номер_счета
code , -- Код_записи_счета
dschet , -- Дата_выставления_счета
dschet_norm , -- Дата_выставления_счета_(нормализованный)
summav , -- Сумма_счета,_выставленная_на_оплату
summap  -- Сумма,_принятая_к_оплате
)
select
3812,
131,
(select uid from dbo.s_users where loginame = session_user),  
cast(to_char(dbo.getWorkDate(), 'YYYYMMDD') as int),
cast(to_char(dbo.getWorkDate(), 'YYYYMMDD') as int),
c_okato1_o002 , -- спр._ОКАТО_территории,_выставившей_счет
okato_oms_o002 , -- спр._ОКАТО_территории_страхования_по_ОМС_(территория,_в_которую_выставляется_счет)
v_filename,
year , -- Отчетный_год
month , -- Отчетный_месяц
nschet , -- Номер_счета
code , -- Код_записи_счета
dschet , -- Дата_выставления_счета
dschet_norm , -- Дата_выставления_счета_(нормализованный)
summav , -- Сумма_счета,_выставленная_на_оплату
summap  -- Сумма,_принятая_к_оплате
from t_d as d
inner join KS_DDLControl.zl_list as t on t.id = v_doc_id 
returning id into v_new_doc_id
;

drop table if exists t_rzl_sl;
drop table if exists t_sl;
drop table if exists t_usl;
drop table if exists t_usl_onk;
drop table if exists t_usl_onk1;
drop table if exists t_usl_onk2;
create temp table if not exists t_rzl_sl (id integer) on commit drop;
create temp table if not exists t_sl (id integer) on commit drop;
create temp table if not exists t_usl (id integer) on commit drop;
create temp table if not exists t_usl_onk (id integer) on commit drop;
create temp table if not exists t_usl_onk1 (id integer) on commit drop;
create temp table if not exists t_usl_onk2 (id integer) on commit drop;

v_num_rzl := 0;

while exists(select * from t_inst0) LOOP
	select ЗСЛ_ID into v_id_rzl from t_inst0 limit 1;

	insert into KS_DDLControl.z_sl ( -- Реестр законченных случаев
	id_origin , -- Исходная_запись_(МТР)
	filename , -- Имя_файла
	p_disp2 , -- Признак_оказания_медицинской_помощи_в_рамках_2_этапа_диспансеризации
	idcase , -- Номер_записи_в_реестре_случаев
	--n_zap , -- Номер_позиции_записи
	usl_ok_v006 , -- Условия_оказания_медицинской_помощи
	vidpom_v008 , -- Вид_медицинской_помощи
	for_pom_v014 , -- Форма_оказания_медицинской_помощи
	npr_mo_f003 , -- МО,_направившая_на_лечение_(диагностику,_консультацию,_госпитализацию)
	npr_date , -- Дата_направления_на_лечение_(диагностику,_консультацию,_госпитализацию)
	lpu_f003 , -- МО
	date_z_1 , -- Дата_начала_лечения
	date_z_2 , -- Дата_окончания_лечения
	kd_z , -- Продолжительность_госпитализации_(койко-дни/пациенто-дни)
	rslt_v009 , -- Результат_обращения
	ishod_v012 , -- Исход_заболевания
	vb_p , -- Признак_внутрибольничного_перевода
	idsp_v010 , -- Способ_оплаты_медицинской_помощи
	sumv , -- Сумма,_выставленная_к_оплате
	oplata_rs005 , -- Тип_оплаты
	sump , -- Сумма,_принятая_к_оплате_ТФОМС
	sank_it  -- Сумма_санкций_по_законченному_случаю
	)
	select 
	inst.id, 
	t_d.Имя_файла, 
	p_disp2 , -- Признак_оказания_медицинской_помощи_в_рамках_2_этапа_диспансеризации
	idcase , -- Номер_записи_в_реестре_случаев
	--n_zap , -- Номер_позиции_записи
	usl_ok_v006 , -- Условия_оказания_медицинской_помощи
	vidpom_v008 , -- Вид_медицинской_помощи
	for_pom_v014 , -- Форма_оказания_медицинской_помощи
	npr_mo_f003 , -- МО,_направившая_на_лечение_(диагностику,_консультацию,_госпитализацию)
	npr_date , -- Дата_направления_на_лечение_(диагностику,_консультацию,_госпитализацию)
	lpu_f003 , -- МО
	date_z_1 , -- Дата_начала_лечения
	date_z_2 , -- Дата_окончания_лечения
	kd_z , -- Продолжительность_госпитализации_(койко-дни/пациенто-дни)
	rslt_v009 , -- Результат_обращения
	ishod_v012 , -- Исход_заболевания
	vb_p , -- Признак_внутрибольничного_перевода
	idsp_v010 , -- Способ_оплаты_медицинской_помощи
	sumv , -- Сумма,_выставленная_к_оплате
	oplata_rs005 , -- Тип_оплаты
	sump , -- Сумма,_принятая_к_оплате_ТФОМС
	sank_it  -- Сумма_санкций_по_законченному_случаю
	from t_d 
	inner join t_inst as inst on 1=1
	inner join KS_DDLControl.z_sl on inst.ЗСЛ_ID = z_sl.id 	
	where inst.ЗСЛ_ID = v_id_rzl
	returning id into v_new_id_rzl
	;
	insert into KS_DDLControl.z_sl_vnov_m ( -- ТЧ Вес при рождении
	id_up,
	vnov_m
	)
	select
	v_new_id_rzl,
	vnov_m
	from KS_DDLControl.z_sl_vnov_m as v 
	inner join t_inst as inst on inst.ЗСЛ_ID = v.id_up 
	where inst.ЗСЛ_ID = v_id_rzl
	;
	insert into KS_DDLControl.z_sl_os_sluch ( -- ТЧ Особый случай
	id_up,
	os_sluch_rs004
	)
	select
	v_new_id_rzl,
	os_sluch_rs004
	from KS_DDLControl.z_sl_os_sluch as v 
	inner join t_inst as inst on inst.ЗСЛ_ID = v.id_up 
	where inst.ЗСЛ_ID = v_id_rzl
	;

	insert into t_rzl_sl 
	select
	sl_s.sl_spr  -- спр. Реестр случаев 
	from KS_DDLControl.z_sl_sl as sl_s
	inner join t_inst as inst on inst.ЗСЛ_ID = sl_s.id_up 
	where inst.ЗСЛ_ID = v_id_rzl
	;

	while exists(select * from t_rzl_sl) loop -- цикл по ТЧ Сведения о случае обрабатываемого элемента "Реестр законченных случаев"
		select id into v_id_sl from t_rzl_sl limit 1;
	
		insert into KS_DDLControl.z_sl ( -- Реестр случаев
		filename , -- Имя_файла
		idcase , -- Идентификатор_законченного_случая
		sl_id , -- Идентификатор_случая
		vid_hmp_v018 , -- Вид_высокотехнологичной_медицинской_помощи
		metod_hmp_v019 , -- Метод_высокотехнологичной_медицинской_помощи
		profil_v002 , -- Профиль_медицинской_помощи
		profil_k_v020 , -- Профиль_койки
		det_rs006 , -- Признаки_детского_профиля
		p_cel_v025 , -- Цель_посещения
		disp , -- Признак_диспансеризации
		tal_d , -- Дата_выдачи_талона_на_ВМП
		nhistory , -- Номер_истории_болезни/талона_амбулаторного_пациента/карты_вызова_скорой_медицинской_помощи
		date_1 , -- Дата_начала_лечения
		date_2 , -- Дата_окончания_лечения
		kd , -- Продолжительность_госпитализации_(койко-дни/пациенто-дни)
		ds0_m001 , -- Диагноз_первичный
		ds1_m001 , -- Диагноз_основной
		c_zab_v027 , -- Характер_основного_заболевания
		ds_onk_rs008 , -- Признак_подозрения_на_злокачественное_новообразование
		dn_rs015 , -- Диспансерное_наблюдение
		mes2_rs021 , -- Cтандарт_медицинской_помощи_сопутствующего_заболевания
		prvs_v021 , -- Специальность_лечащего_врача/врача,_закрывшего_талон/историю_болезни
		vers_spec , -- Код_классификатора_медицинских_специальностей
		ed_col , -- Количество_единиц_оплаты_медицинской_помощи
		tarif , -- Тариф
		sum_m , -- Стоимость_случая,_выставленная_к_оплате
		onk_sl_ds1_t_n018 , -- Повод_обращения
		onk_sl_stad_n002 , -- Стадия_заболевания
		onk_sl_onk_t_n003 , -- Tumor
		onk_sl_onk_n_n004 , -- Nodus
		onk_sl_onk_m_n005 , -- Metastasis
		onk_sl_mtstz , -- Признак_выявления_отдаленных_метастазов
		onk_sl_sod , -- Суммарная_очаговая_доза
		onk_sl_k_fr , -- Количество_фракций_проведения_лучевой_терапии
		onk_sl_wei , -- Масса_тела_(кг)
		onk_sl_hei , -- Рост_(см)
		onk_sl_bsa , -- Площадь_поверхности_тела_(м2)
		ksg_kpg_n_ksg_v023 , -- Классификатор_клинико-статистических_групп
		ksg_kpg_ver_ksg , -- Модель_определения_КСГ
		ksg_kpg_ksg_pg_rs010 , -- Признак_использования_подгруппы_КСГ
		ksg_kpg_n_kpg_v026 , -- Классификатор_клинико-профильных_групп
		ksg_kpg_koef_z , -- Коэффициент_затратоемкости
		ksg_kpg_koef_up , -- Управленческий_коэффициент
		ksg_kpg_bztsz , -- Базовая_ставка
		ksg_kpg_koef_d , -- Коэффициент_дифференциации
		ksg_kpg_koef_u , -- Коэффициент_уровня/подуровня_оказания_медицинской_помощи
		ksg_kpg_sl_k_rs011 , -- Признаки_использования_КСЛП
		ksg_kpg_it_sl  -- Примененный_коэффициент_сложности_лечения_пациента
		)
		select 
		v_filename, 
		idcase , -- Идентификатор_законченного_случая
		sl_id , -- Идентификатор_случая
		vid_hmp_v018 , -- Вид_высокотехнологичной_медицинской_помощи
		metod_hmp_v019 , -- Метод_высокотехнологичной_медицинской_помощи
		profil_v002 , -- Профиль_медицинской_помощи
		profil_k_v020 , -- Профиль_койки
		det_rs006 , -- Признаки_детского_профиля
		p_cel_v025 , -- Цель_посещения
		disp , -- Признак_диспансеризации,
		tal_d , -- Дата_выдачи_талона_на_ВМП
		nhistory , -- Номер_истории_болезни/талона_амбулаторного_пациента/карты_вызова_скорой_медицинской_помощи
		date_1 , -- Дата_начала_лечения
		date_2 , -- Дата_окончания_лечения
		kd , -- Продолжительность_госпитализации_(койко-дни/пациенто-дни)
		ds0_m001 , -- Диагноз_первичный
		ds1_m001 , -- Диагноз_основной
		c_zab_v027 , -- Характер_основного_заболевания
		ds_onk_rs008 , -- Признак_подозрения_на_злокачественное_новообразование
		dn_rs015 , -- Диспансерное_наблюдение
		mes2_rs021 , -- Cтандарт_медицинской_помощи_сопутствующего_заболевания
		prvs_v021 , -- Специальность_лечащего_врача/врача,_закрывшего_талон/историю_болезни
		vers_spec , -- Код_классификатора_медицинских_специальностей
		ed_col , -- Количество_единиц_оплаты_медицинской_помощи
		tarif , -- Тариф
		sum_m , -- Стоимость_случая,_выставленная_к_оплате
		onk_sl_ds1_t_n018 , -- Повод_обращения
		onk_sl_stad_n002 , -- Стадия_заболевания
		onk_sl_onk_t_n003 , -- Tumor
		onk_sl_onk_n_n004 , -- Nodus
		onk_sl_onk_m_n005 , -- Metastasis
		onk_sl_mtstz , -- Признак_выявления_отдаленных_метастазов
		onk_sl_sod , -- Суммарная_очаговая_доза
		onk_sl_k_fr , -- Количество_фракций_проведения_лучевой_терапии
		onk_sl_wei , -- Масса_тела_(кг)
		onk_sl_hei , -- Рост_(см)
		onk_sl_bsa , -- Площадь_поверхности_тела_(м2)
		ksg_kpg_n_ksg_v023 , -- Классификатор_клинико-статистических_групп
		ksg_kpg_ver_ksg , -- Модель_определения_КСГ
		ksg_kpg_ksg_pg_rs010 , -- Признак_использования_подгруппы_КСГ
		ksg_kpg_n_kpg_v026 , -- Классификатор_клинико-профильных_групп
		ksg_kpg_koef_z , -- Коэффициент_затратоемкости
		ksg_kpg_koef_up , -- Управленческий_коэффициент
		ksg_kpg_bztsz , -- Базовая_ставка
		ksg_kpg_koef_d , -- Коэффициент_дифференциации
		ksg_kpg_koef_u , -- Коэффициент_уровня/подуровня_оказания_медицинской_помощи
		ksg_kpg_sl_k_rs011 , -- Признаки_использования_КСЛП
		ksg_kpg_it_sl  -- Примененный_коэффициент_сложности_лечения_пациента
		from t_d 
		inner join KS_DDLControl.sl as sl on sl.id = v_id_sl -- спр. Реестр случаев 
		returning id into v_new_id_sl
		;
		insert into KS_DDLControl.sl_ds2 ( -- ТЧ Диагнозы сопутствующих заболеваний
		id_up,
		ds2_m001
		)
		select
		v_new_id_sl,
		ds2_m001
		from KS_DDLControl.sl_ds2 as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_ds3 ( -- ТЧ Диагнозы осложнений заболевания
		id_up,
		ds3_m001
		)
		select
		v_new_id_sl,
		ds3_m001
		from KS_DDLControl.sl_ds3 as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_mes1 ( -- ТЧ Стандарты медицинской помощи
		id_up,
		mes1_rs021
		)
		select
		v_new_id_sl,
		mes1_rs021
		from KS_DDLControl.sl_mes1 as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_napr ( -- ТЧ Сведения об оформлении направления
		id_up,
		napr_usl_v001 , -- Медицинская_услуга,_указанная_в_направлении
		met_issl_v029 , -- Метод_диагностического_исследования
		napr_v_v028 , -- Вид_направления
		napr_mo_f003 , -- МО,_куда_оформлено_направление
		napr_date  -- Дата_направления
		)
		select
		v_new_id_sl,
		napr_usl_v001 , -- Медицинская_услуга,_указанная_в_направлении
		met_issl_v029 , -- Метод_диагностического_исследования
		napr_v_v028 , -- Вид_направления
		napr_mo_f003 , -- МО,_куда_оформлено_направление
		napr_date  -- Дата_направления
		from KS_DDLControl.sl_napr as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_cons ( -- ТЧ Сведения о проведении консилиума
		id_up,
		pr_cons_n019 , -- Цель_проведения_консилиума
		dt_cons  -- Дата_проведения_консилиума
		)
		select
		v_new_id_sl,
		pr_cons_n019 , -- Цель_проведения_консилиума
		dt_cons  -- Дата_проведения_консилиума
		from KS_DDLControl.sl_cons as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_onk_sl_b_diag ( -- ТЧ Диагностический блок
		id_up,
		diag_rslt_n008 , -- Результат_диагностики
		diag_code_n007 , -- Диагностический_показатель
		diag_code_n010 , -- Диагностический_показатель_(маркер)
		diag_rslt_n011 , -- Результат_диагностики_(значение_маркера)
		diag_tip_rs033 , -- Тип_диагностического_показателя
		rec_rslt , -- Признак_получения_результата_диагностики
		diag_date  -- Дата_взятия_материала
		)
		select
		v_new_id_sl,
		diag_rslt_n008 , -- Результат_диагностики
		diag_code_n007 , -- Диагностический_показатель
		diag_code_n010 , -- Диагностический_показатель_(маркер)
		diag_rslt_n011 , -- Результат_диагностики_(значение_маркера)
		diag_tip_rs033 , -- Тип_диагностического_показателя
		rec_rslt , -- Признак_получения_результата_диагностики
		diag_date  -- Дата_взятия_материала
		from KS_DDLControl.sl_onk_sl_b_diag as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_onk_sl_b_prot ( -- ТЧ Сведения об имеющихся противопоказаниях и отказах
		id_up,
		prot_n001 , -- Противопоказание_или_отказ
		d_prot  -- Дата_регистрации_противопоказания_или_отказа
		)
		select
		v_new_id_sl,
		prot_n001 , -- Противопоказание_или_отказ
		d_prot  -- Дата_регистрации_противопоказания_или_отказа
		from KS_DDLControl.sl_onk_sl_b_prot as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_ksg_kpg_crit ( -- ТЧ Классификационные критерии
		id_up,
		crit_v024  -- Классификатор_классификационных_критериев
		)
		select
		v_new_id_sl,
		crit_v024  -- Классификатор_классификационных_критериев
		from KS_DDLControl.sl_ksg_kpg_crit as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_ksg_kpg_sl_koef ( -- ТЧ Коэффициенты сложности лечения пациента
		id_up,
		z_sl , -- Значение_коэффициента_сложности_лечения_пациента
		idsl  -- Номер_коэффициента_сложности_лечения_пациента
		)
		select
		v_new_id_sl,
		z_sl , -- Значение_коэффициента_сложности_лечения_пациента
		idsl  -- Номер_коэффициента_сложности_лечения_пациента
		from KS_DDLControl.sl_ksg_kpg_sl_koef as t
		where t.id_up = v_id_sl
		;

		insert into t_usl_onk
		select
		id
		from KS_DDLControl.sl_onk_sl_onk_usl as t
		where t.id_up = v_id_sl
		;
	
		while exists(select * from t_usl_onk) loop -- цикл по ТЧ Сведения об услуге при лечении онкологического заболевания.
			select id into v_id_usl_onk from t_usl_onk limit 1;
	
			insert into KS_DDLControl.sl_onk_sl_onk_usl ( 
			id_up,
			luch_tip_n017 , -- Тип_лучевой_терапии
			lek_tip_v_n016 , -- Цикл_лекарственной_терапии
			lek_tip_l_n015 , -- Линия_лекарственной_терапии
			hir_tip_n014 , -- Тип_хирургического_лечения
			usl_tip_n013 , -- Тип_услуги
			pptr  -- Признак_проведения_профилактики_тошноты_и_рвотного_рефлекса
			)
			select
			v_new_id_sl,
			luch_tip_n017 , -- Тип_лучевой_терапии
			lek_tip_v_n016 , -- Цикл_лекарственной_терапии
			lek_tip_l_n015 , -- Линия_лекарственной_терапии
			hir_tip_n014 , -- Тип_хирургического_лечения
			usl_tip_n013 , -- Тип_услуги
			pptr  -- Признак_проведения_профилактики_тошноты_и_рвотного_рефлекса
			from KS_DDLControl.sl_onk_sl_onk_usl as t
			where t.id = v_id_usl_onk
			returning id into v_new_id_usl_onk
			;
		
			insert into t_usl_onk1
			select
			id
			from KS_DDLControl.sl_onk_sl_onk_usl_lek_pr as t
			where t.id_up = v_id_usl_onk
			;
			while exists(select * from t_usl_onk1) loop -- цикл по ТЧ Сведения о введенном противоопухолевом лекарственном препарате
				select id into v_id_usl_onk1 from t_usl_onk limit 1;
			
				insert into KS_DDLControl.sl_onk_sl_onk_usl_lek_pr (
				id_up,
				regnum_n020 , -- Классификатор_лекарственных_препаратов,_применяемых_при_проведении_лекарственной_терапии
				code_sh_v024  -- Классификатор_классификационных_критериев
				)
				select
				v_id_usl_onk,
				regnum_n020 , -- Классификатор_лекарственных_препаратов,_применяемых_при_проведении_лекарственной_терапии
				code_sh_v024  -- Классификатор_классификационных_критериев
				from KS_DDLControl.sl_onk_sl_onk_usl_lek_pr as t
				where t.id = v_id_usl_onk1
				returning id into v_new_id_usl_onk1
				;
				insert into KS_DDLControl.sl_onk_sl_onk_usl_lek_pr_date_inj ( -- ТЧ Даты введения лекарственного препарата
				id_up,
				date_inj  -- Дата_введения_лекарственного_препарата
				)
				select
				v_new_id_usl_onk1,
				date_inj  -- Дата_введения_лекарственного_препарата
				from KS_DDLControl.sl_onk_sl_b_prot as t
				where t.id_up = v_id_usl_onk1			
				;
			
				delete from t_usl_onk1 where id = v_id_usl_onk1;
			END loop
			;
	
			delete from t_usl_onk where id = v_id_usl_onk;
		END loop
		;

		insert into t_usl
		select
		usl_spr -- спр. Реестр оказанных услуг
		from KS_DDLControl.sl_usl as t
		where t.id_up = v_id_sl
		;
		while exists(select * from t_usl) loop -- цикл по ТЧ Сведения об услуге при лечении онкологического заболевания.
			select id into v_id_usl from t_usl limit 1;

			insert into KS_DDLControl.usl ( -- Реестр оказанных услуг
			filename , -- Имя_файла
			idcase , -- Идентификатор_законченного_случая
			sl_id , -- Идентификатор_случая
			idserv , -- Идентификатор
			pacient , -- Сведения_о_пациенте
			lpu , -- МО
			profil , -- Профиль_медицинской_помощи
			vid_vme , -- Вид_медицинского_вмешательства
			det , -- Признак_детского_профиля
			date_in , -- Дата_начала_оказания_услуги
			date_out , -- Дата_окончания_оказания_услуги
			ds , -- Диагноз_(МКБ-10)
			code_usl , -- Номенклатура_работ_и_услуг_в_здравоохранении_(региональный_перечень)
			kol_usl , -- Количество_услуг_(кратность_услуги)
			tarif , -- Тариф
			sumv_usl , -- Стоимость_медицинской_услуги,_выставленная_к_оплате_(руб.)
			prvs  -- Специальность_медработника,_выполнившего_услугу
			)
			select 
			v_filename, 
			idcase , -- Идентификатор_законченного_случая
			sl_id , -- Идентификатор_случая
			idserv , -- Идентификатор
			pacient , -- Сведения_о_пациенте
			lpu , -- МО
			profil , -- Профиль_медицинской_помощи
			vid_vme , -- Вид_медицинского_вмешательства
			det , -- Признак_детского_профиля
			date_in , -- Дата_начала_оказания_услуги
			date_out , -- Дата_окончания_оказания_услуги
			ds , -- Диагноз_(МКБ-10)
			code_usl , -- Номенклатура_работ_и_услуг_в_здравоохранении_(региональный_перечень)
			kol_usl , -- Количество_услуг_(кратность_услуги)
			tarif , -- Тариф
			sumv_usl , -- Стоимость_медицинской_услуги,_выставленная_к_оплате_(руб.)
			prvs  -- Специальность_медработника,_выполнившего_услугу
			from t_d 
			inner join KS_DDLControl.usl on usl.id = v_id_usl
			returning id into v_new_id_usl 
			;
			
			-- добавляем в ТЧ Сведения о случае созданный элемент спр. Реестр оказанных услуг
			insert into	KS_DDLControl.sl_usl ( id_up, usl_spr ) values ( v_new_id_sl, v_new_id_usl ) 
			;

			delete from t_usl where id = v_id_usl;
		END loop
		;
		-- добавляем в ТЧ Сведения о случае созданный элемент спр. Реестр случаев 
		insert into	KS_DDLControl.z_sl_sl ( id_up, sl_spr ) values ( v_new_id_rzl, v_new_id_sl ) 
		;
	
		delete from t_rzl_sl where id = v_id_sl;
	END loop
	;

	-- получаем максимальный порядковый номер записи этой в ТЧ Записи текущего документа
	select max(n_zap) into v_num_rzl from KS_DDLControl.zl_list_zap where id_up = v_new_doc_id
	;
	-- добавляем в ТЧ Записи текущего документа созданный элемент спр. Реестр законченных случаев 
	insert into	KS_DDLControl.zl_list_zap ( 
	id_up, 
	pacient_spr , -- Сведения_о_пациенте_(идентифицированный)
	z_sl_spr,  -- Реестр_законченных_случаев
	correction  -- Исправленние
	) 
	select
	v_new_doc_id , 
	ПАЦИЕНТ_ID,
	v_new_id_rzl,
	0
	from t_inst
	where ЗСЛ_ID = v_id_rzl
	returning id into v_new_id_zap
	;
	-- пишем его в созданный элемент в поле Номер позиции записи
	update KS_DDLControl.zl_list_zap
	set n_zap = coalesce(v_num_rzl, 0) + 1
	where id = v_new_id_zap
	;
	update KS_DDLControl.z_sl
	set n_zap = coalesce(v_num_rzl, 0) + 1
	where id = v_new_id_rzl
	;

	delete from t_inst0 where ЗСЛ_ID = v_id_rzl;
END loop
;

update ks_ddlcontrol.zl_list_zap   -- ТЧ Записи
set correction = 0
where id_up = v_doc_id and correction = 1
;

drop table if exists KS_DDLControl.uk_3093;
perform dbo.cl_unique_trg (v_nentityid := 3093, v_protrid := NULL);
drop table if exists KS_DDLControl.uk_3136;
perform dbo.cl_unique_trg (v_nentityid := 3136, v_protrid := NULL);
drop table if exists KS_DDLControl.uk_3139;
perform dbo.cl_unique_trg (v_nentityid := 3139, v_protrid := NULL);


end 
--$$;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3107_3812_fill_data_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- 9.2 ДЕЙСТВИЕ «Заполнить исходными данными» для документа «Файлы реестров счетов по оплате медицинских услуг (R-файл и D-файл –  Исходящий МТР)»

declare
--v_doc_id integer;
v_Unit uuid;
v_old_cnt integer;
v_new_cnt integer;

v_num_rzl integer;
v_new_id_zap integer;

v_id_rzl integer;
v_id_sl integer;
v_id_usl integer;
v_id_usl_onk integer;
v_id_usl_onk1 integer;
v_id_usl_onk2 integer;

v_new_id_rzl integer;
v_new_id_sl integer;
v_new_id_usl integer;
v_new_id_usl_onk integer;
v_new_id_usl_onk1 integer;
v_new_id_usl_onk2 integer;

begin 

--v_doc_id := 100;

select dbo.sys_guid() 
into v_Unit;
-- select * from KS_DDLControl.zl_list where left(filename, 1)='R'  

v_old_cnt := 0;

create temporary table t_d on commit drop as -- текущий документ
select 
d.status as Статус,
filename as Имя_файла,
data_norm as Дата,
left(cast(data_norm as varchar(8)), 4) as Y,
o002.ter as Территория_КОМУ
from KS_DDLControl.zl_list as d -- Сведения об оказанной МП / Файлы реестров счетов по оплате медицинских услуг (R-файл и D-файл –  Исходящий МТР)
inner join ks_ddlcontrol.o002 ON o002.id = d.okato_oms_o002
where d.id = v_doc_id
;
if not exists (select * from t_d where left(coalesce(Имя_файла,''), 1)='R' ) then
	RAISE EXCEPTION 'Заполнение данными воможно только для основной части реестра счетов по оплате медицинских услуг';
end if
;
if not exists (select * from t_d where Статус = 131) then
	RAISE EXCEPTION 'Заполнение данными воможно только для документов на статусе “Новый”';
end if
;
create temporary table t_inst on commit drop as -- исходные данные
select 
d.id,
d.status as Статус,
d.filename as Имя_файла,
d.data_norm as Дата,
left(cast(d.data_norm as varchar(8)), 4) as Год,
d.disp_v016 as Тип_диспансеризации,
zap.pacient_spr as ПАЦИЕНТ_ID, --Сведения_о_пациенте
o002.ter as Регион_пациента,
zap.z_sl_spr as ЗСЛ_ID -- Реестр_законченных_случаев
from KS_DDLControl.zl_list as d -- Сведения об оказанной МП / «Сведения об оказанной медицинской помощи (H файл)» (id 3721)
															--«Сведения об оказанной медицинской помощи (T файл)» (id 3733)
															--«Сведения об оказанной медицинской помощи (X файл)» (id 3734)
															--«Сведения об оказанной медицинской помощи (C файл)» (id 3735)

inner join ks_ddlcontrol.zl_list_zap as zap on d.id = zap.id_up -- ТЧ Записи
inner join ks_ddlcontrol.tbl_pacient as pac on pac.id = zap.pacient_spr -- Сведения о пациенте (идентифицированный)
inner join ks_ddlcontrol.o002 as o002 on pac.id = pac.st_okato_o002 -- спр. Регион страхования
inner join t_d as d0 on d0.Территория_КОМУ = o002.ter -- Регион_пациента
where d.documentid in (3721, 3733, 3734, 3735) and status in (136, 137, 138, 139)
;
create temporary table t_podd on commit drop as -- 3.	Поданные_данные
select 
d.status as Статус,
d.filename as Имя_файла,
data_norm as Дата,
left(cast(data_norm as varchar(8)), 4) as Y,
o002.ter as Территория_КОМУ,
zap.pacient_spr as ПАЦИЕНТ_ID, --Сведения_о_пациенте
z_sl_spr as ЗСЛ_ID, -- Реестр_законченных_случаев
id_origin as Исходный_ЗСЛ_ID -- Исходная запись (МТР)
from KS_DDLControl.zl_list as d -- Сведения об оказанной МП / Файлы реестров счетов по оплате медицинских услуг (R-файл и D-файл –  Исходящий МТР)
inner join ks_ddlcontrol.zl_list_zap as zap on d.id = zap.id_up -- ТЧ Записи
inner join KS_DDLControl.z_sl on z_sl.id = zap.z_sl_spr 
inner join ks_ddlcontrol.o002 ON o002.id = d.okato_oms_o002
inner join t_d as d0 on d0.Территория_КОМУ = o002.ter -- Территория_КОМУ
where d.documentid = 3812 and status <> 140
;
-- 3.	Из Запрос. Исходные_данные исключить строки для которых Запрос. Исходные_данные. ЗСЛ_ID = Запрос Поданные_данные.Исходный_ЗСЛ_ID
delete from t_inst where ЗСЛ_ID in (select Исходный_ЗСЛ_ID from t_podd)
;
if not exists (select * from t_inst) then
	RAISE EXCEPTION 'Среди поданных и необработанных сведений о МП записи для данного региона отсутствуют';
--	RAISE notice 'Среди поданных и необработанных сведений о МП записи для данного региона отсутствуют';
end if
;

create temporary table t_inst0 on commit drop as -- исходные данные - для цикла
select *
from t_inst
;

drop table if exists t_rzl_sl;
drop table if exists t_sl;
drop table if exists t_usl;
drop table if exists t_usl_onk;
drop table if exists t_usl_onk1;
drop table if exists t_usl_onk2;
create temp table if not exists t_rzl_sl (id integer) on commit drop;
create temp table if not exists t_sl (id integer) on commit drop;
create temp table if not exists t_usl (id integer) on commit drop;
create temp table if not exists t_usl_onk (id integer) on commit drop;
create temp table if not exists t_usl_onk1 (id integer) on commit drop;
create temp table if not exists t_usl_onk2 (id integer) on commit drop;

v_num_rzl := 0;

-- 5.	Для каждой записи оставшейся после шага 3 создать новую запись в справочнике «Реестр законченных случаев» согласно Таблица 9.2.1 
-- и записать в ТЧ Записи текущего документа новый элемент новый элемент справочника «Реестр законченных случаев» 
-- в паре с Запрос Исходные_данные. ПАЦИЕНТ_ID для исходной
while exists(select * from t_inst0) LOOP
	select ЗСЛ_ID into v_id_rzl from t_inst0 limit 1;

	insert into KS_DDLControl.z_sl ( -- Реестр законченных случаев
	id_origin , -- Исходная_запись_(МТР)
	filename , -- Имя_файла
	p_disp2 , -- Признак_оказания_медицинской_помощи_в_рамках_2_этапа_диспансеризации
	idcase , -- Номер_записи_в_реестре_случаев
	--n_zap , -- Номер_позиции_записи
	usl_ok_v006 , -- Условия_оказания_медицинской_помощи
	vidpom_v008 , -- Вид_медицинской_помощи
	for_pom_v014 , -- Форма_оказания_медицинской_помощи
	npr_mo_f003 , -- МО,_направившая_на_лечение_(диагностику,_консультацию,_госпитализацию)
	npr_date , -- Дата_направления_на_лечение_(диагностику,_консультацию,_госпитализацию)
	lpu_f003 , -- МО
	date_z_1 , -- Дата_начала_лечения
	date_z_2 , -- Дата_окончания_лечения
	kd_z , -- Продолжительность_госпитализации_(койко-дни/пациенто-дни)
	rslt_v009 , -- Результат_обращения
	ishod_v012 , -- Исход_заболевания
	vb_p , -- Признак_внутрибольничного_перевода
	idsp_v010 , -- Способ_оплаты_медицинской_помощи
	sumv , -- Сумма,_выставленная_к_оплате
	oplata_rs005 , -- Тип_оплаты
	sump , -- Сумма,_принятая_к_оплате_ТФОМС
	sank_it  -- Сумма_санкций_по_законченному_случаю
	)
	select 
	inst.id, 
	t_d.Имя_файла, 
	case when inst.Тип_диспансеризации in (17, 22, 23, 29) then 1 else null end,
	idcase , -- Номер_записи_в_реестре_случаев
	--n_zap , -- Номер_позиции_записи
	usl_ok_v006 , -- Условия_оказания_медицинской_помощи
	vidpom_v008 , -- Вид_медицинской_помощи
	for_pom_v014 , -- Форма_оказания_медицинской_помощи
	npr_mo_f003 , -- МО,_направившая_на_лечение_(диагностику,_консультацию,_госпитализацию)
	npr_date , -- Дата_направления_на_лечение_(диагностику,_консультацию,_госпитализацию)
	lpu_f003 , -- МО
	date_z_1 , -- Дата_начала_лечения
	date_z_2 , -- Дата_окончания_лечения
	kd_z , -- Продолжительность_госпитализации_(койко-дни/пациенто-дни)
	rslt_v009 , -- Результат_обращения
	ishod_v012 , -- Исход_заболевания
	vb_p , -- Признак_внутрибольничного_перевода
	idsp_v010 , -- Способ_оплаты_медицинской_помощи
	sumv , -- Сумма,_выставленная_к_оплате
	oplata_rs005 , -- Тип_оплаты
	sump , -- Сумма,_принятая_к_оплате_ТФОМС
	sank_it  -- Сумма_санкций_по_законченному_случаю
	from t_d 
	inner join t_inst as inst on 1=1
	inner join KS_DDLControl.z_sl on inst.ЗСЛ_ID = z_sl.id 	
	where inst.ЗСЛ_ID = v_id_rzl
	returning id into v_new_id_rzl
	;
	insert into KS_DDLControl.z_sl_vnov_m ( -- ТЧ Вес при рождении
	id_up,
	vnov_m
	)
	select
	v_new_id_rzl,
	vnov_m
	from KS_DDLControl.z_sl_vnov_m as v 
	inner join t_inst as inst on inst.ЗСЛ_ID = v.id_up 
	where inst.ЗСЛ_ID = v_id_rzl
	;
	insert into KS_DDLControl.z_sl_os_sluch ( -- ТЧ Особый случай
	id_up,
	os_sluch_rs004
	)
	select
	v_new_id_rzl,
	os_sluch_rs004
	from KS_DDLControl.z_sl_os_sluch as v 
	inner join t_inst as inst on inst.ЗСЛ_ID = v.id_up 
	where inst.ЗСЛ_ID = v_id_rzl
	;

	insert into t_rzl_sl 
	select
	sl_s.sl_spr  -- спр. Реестр случаев 
	from KS_DDLControl.z_sl_sl as sl_s
	inner join t_inst as inst on inst.ЗСЛ_ID = sl_s.id_up 
	where inst.ЗСЛ_ID = v_id_rzl
	;

	while exists(select * from t_rzl_sl) loop -- цикл по ТЧ Сведения о случае обрабатываемого элемента "Реестр законченных случаев"
		select id into v_id_sl from t_rzl_sl limit 1;
	
		insert into KS_DDLControl.z_sl ( -- Реестр случаев
		filename , -- Имя_файла
		idcase , -- Идентификатор_законченного_случая
		sl_id , -- Идентификатор_случая
		vid_hmp_v018 , -- Вид_высокотехнологичной_медицинской_помощи
		metod_hmp_v019 , -- Метод_высокотехнологичной_медицинской_помощи
		profil_v002 , -- Профиль_медицинской_помощи
		profil_k_v020 , -- Профиль_койки
		det_rs006 , -- Признаки_детского_профиля
		p_cel_v025 , -- Цель_посещения
		disp , -- Признак_диспансеризации
		tal_d , -- Дата_выдачи_талона_на_ВМП
		nhistory , -- Номер_истории_болезни/талона_амбулаторного_пациента/карты_вызова_скорой_медицинской_помощи
		date_1 , -- Дата_начала_лечения
		date_2 , -- Дата_окончания_лечения
		kd , -- Продолжительность_госпитализации_(койко-дни/пациенто-дни)
		ds0_m001 , -- Диагноз_первичный
		ds1_m001 , -- Диагноз_основной
		c_zab_v027 , -- Характер_основного_заболевания
		ds_onk_rs008 , -- Признак_подозрения_на_злокачественное_новообразование
		dn_rs015 , -- Диспансерное_наблюдение
		mes2_rs021 , -- Cтандарт_медицинской_помощи_сопутствующего_заболевания
		prvs_v021 , -- Специальность_лечащего_врача/врача,_закрывшего_талон/историю_болезни
		vers_spec , -- Код_классификатора_медицинских_специальностей
		ed_col , -- Количество_единиц_оплаты_медицинской_помощи
		tarif , -- Тариф
		sum_m , -- Стоимость_случая,_выставленная_к_оплате
		onk_sl_ds1_t_n018 , -- Повод_обращения
		onk_sl_stad_n002 , -- Стадия_заболевания
		onk_sl_onk_t_n003 , -- Tumor
		onk_sl_onk_n_n004 , -- Nodus
		onk_sl_onk_m_n005 , -- Metastasis
		onk_sl_mtstz , -- Признак_выявления_отдаленных_метастазов
		onk_sl_sod , -- Суммарная_очаговая_доза
		onk_sl_k_fr , -- Количество_фракций_проведения_лучевой_терапии
		onk_sl_wei , -- Масса_тела_(кг)
		onk_sl_hei , -- Рост_(см)
		onk_sl_bsa , -- Площадь_поверхности_тела_(м2)
		ksg_kpg_n_ksg_v023 , -- Классификатор_клинико-статистических_групп
		ksg_kpg_ver_ksg , -- Модель_определения_КСГ
		ksg_kpg_ksg_pg_rs010 , -- Признак_использования_подгруппы_КСГ
		ksg_kpg_n_kpg_v026 , -- Классификатор_клинико-профильных_групп
		ksg_kpg_koef_z , -- Коэффициент_затратоемкости
		ksg_kpg_koef_up , -- Управленческий_коэффициент
		ksg_kpg_bztsz , -- Базовая_ставка
		ksg_kpg_koef_d , -- Коэффициент_дифференциации
		ksg_kpg_koef_u , -- Коэффициент_уровня/подуровня_оказания_медицинской_помощи
		ksg_kpg_sl_k_rs011 , -- Признаки_использования_КСЛП
		ksg_kpg_it_sl  -- Примененный_коэффициент_сложности_лечения_пациента
		)
		select 
		t.Имя_файла, 
		idcase , -- Идентификатор_законченного_случая
		sl_id , -- Идентификатор_случая
		vid_hmp_v018 , -- Вид_высокотехнологичной_медицинской_помощи
		metod_hmp_v019 , -- Метод_высокотехнологичной_медицинской_помощи
		profil_v002 , -- Профиль_медицинской_помощи
		profil_k_v020 , -- Профиль_койки
		det_rs006 , -- Признаки_детского_профиля
		p_cel_v025 , -- Цель_посещения
		case when left(filename, 2) in ('DP', 'DV', 'DO', 'DS', 'DU', 'DF') then 1 else 0 end,
		tal_d , -- Дата_выдачи_талона_на_ВМП
		nhistory , -- Номер_истории_болезни/талона_амбулаторного_пациента/карты_вызова_скорой_медицинской_помощи
		date_1 , -- Дата_начала_лечения
		date_2 , -- Дата_окончания_лечения
		kd , -- Продолжительность_госпитализации_(койко-дни/пациенто-дни)
		ds0_m001 , -- Диагноз_первичный
		ds1_m001 , -- Диагноз_основной
		c_zab_v027 , -- Характер_основного_заболевания
		ds_onk_rs008 , -- Признак_подозрения_на_злокачественное_новообразование
		dn_rs015 , -- Диспансерное_наблюдение
		mes2_rs021 , -- Cтандарт_медицинской_помощи_сопутствующего_заболевания
		prvs_v021 , -- Специальность_лечащего_врача/врача,_закрывшего_талон/историю_болезни
		vers_spec , -- Код_классификатора_медицинских_специальностей
		ed_col , -- Количество_единиц_оплаты_медицинской_помощи
		tarif , -- Тариф
		sum_m , -- Стоимость_случая,_выставленная_к_оплате
		onk_sl_ds1_t_n018 , -- Повод_обращения
		onk_sl_stad_n002 , -- Стадия_заболевания
		onk_sl_onk_t_n003 , -- Tumor
		onk_sl_onk_n_n004 , -- Nodus
		onk_sl_onk_m_n005 , -- Metastasis
		onk_sl_mtstz , -- Признак_выявления_отдаленных_метастазов
		onk_sl_sod , -- Суммарная_очаговая_доза
		onk_sl_k_fr , -- Количество_фракций_проведения_лучевой_терапии
		onk_sl_wei , -- Масса_тела_(кг)
		onk_sl_hei , -- Рост_(см)
		onk_sl_bsa , -- Площадь_поверхности_тела_(м2)
		ksg_kpg_n_ksg_v023 , -- Классификатор_клинико-статистических_групп
		ksg_kpg_ver_ksg , -- Модель_определения_КСГ
		ksg_kpg_ksg_pg_rs010 , -- Признак_использования_подгруппы_КСГ
		ksg_kpg_n_kpg_v026 , -- Классификатор_клинико-профильных_групп
		ksg_kpg_koef_z , -- Коэффициент_затратоемкости
		ksg_kpg_koef_up , -- Управленческий_коэффициент
		ksg_kpg_bztsz , -- Базовая_ставка
		ksg_kpg_koef_d , -- Коэффициент_дифференциации
		ksg_kpg_koef_u , -- Коэффициент_уровня/подуровня_оказания_медицинской_помощи
		ksg_kpg_sl_k_rs011 , -- Признаки_использования_КСЛП
		ksg_kpg_it_sl  -- Примененный_коэффициент_сложности_лечения_пациента
		from t_d 
		inner join KS_DDLControl.sl as sl on sl.id = v_id_sl -- спр. Реестр случаев 
		returning id into v_new_id_sl
		;
		insert into KS_DDLControl.sl_ds2 ( -- ТЧ Диагнозы сопутствующих заболеваний
		id_up,
		ds2_m001
		)
		select
		v_new_id_sl,
		ds2_m001
		from KS_DDLControl.sl_ds2 as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_ds3 ( -- ТЧ Диагнозы осложнений заболевания
		id_up,
		ds3_m001
		)
		select
		v_new_id_sl,
		ds3_m001
		from KS_DDLControl.sl_ds3 as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_mes1 ( -- ТЧ Стандарты медицинской помощи
		id_up,
		mes1_rs021
		)
		select
		v_new_id_sl,
		mes1_rs021
		from KS_DDLControl.sl_mes1 as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_napr ( -- ТЧ Сведения об оформлении направления
		id_up,
		napr_usl_v001 , -- Медицинская_услуга,_указанная_в_направлении
		met_issl_v029 , -- Метод_диагностического_исследования
		napr_v_v028 , -- Вид_направления
		napr_mo_f003 , -- МО,_куда_оформлено_направление
		napr_date  -- Дата_направления
		)
		select
		v_new_id_sl,
		napr_usl_v001 , -- Медицинская_услуга,_указанная_в_направлении
		met_issl_v029 , -- Метод_диагностического_исследования
		napr_v_v028 , -- Вид_направления
		napr_mo_f003 , -- МО,_куда_оформлено_направление
		napr_date  -- Дата_направления
		from KS_DDLControl.sl_napr as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_cons ( -- ТЧ Сведения о проведении консилиума
		id_up,
		pr_cons_n019 , -- Цель_проведения_консилиума
		dt_cons  -- Дата_проведения_консилиума
		)
		select
		v_new_id_sl,
		pr_cons_n019 , -- Цель_проведения_консилиума
		dt_cons  -- Дата_проведения_консилиума
		from KS_DDLControl.sl_cons as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_onk_sl_b_diag ( -- ТЧ Диагностический блок
		id_up,
		diag_rslt_n008 , -- Результат_диагностики
		diag_code_n007 , -- Диагностический_показатель
		diag_code_n010 , -- Диагностический_показатель_(маркер)
		diag_rslt_n011 , -- Результат_диагностики_(значение_маркера)
		diag_tip_rs033 , -- Тип_диагностического_показателя
		rec_rslt , -- Признак_получения_результата_диагностики
		diag_date  -- Дата_взятия_материала
		)
		select
		v_new_id_sl,
		diag_rslt_n008 , -- Результат_диагностики
		diag_code_n007 , -- Диагностический_показатель
		diag_code_n010 , -- Диагностический_показатель_(маркер)
		diag_rslt_n011 , -- Результат_диагностики_(значение_маркера)
		diag_tip_rs033 , -- Тип_диагностического_показателя
		rec_rslt , -- Признак_получения_результата_диагностики
		diag_date  -- Дата_взятия_материала
		from KS_DDLControl.sl_onk_sl_b_diag as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_onk_sl_b_prot ( -- ТЧ Сведения об имеющихся противопоказаниях и отказах
		id_up,
		prot_n001 , -- Противопоказание_или_отказ
		d_prot  -- Дата_регистрации_противопоказания_или_отказа
		)
		select
		v_new_id_sl,
		prot_n001 , -- Противопоказание_или_отказ
		d_prot  -- Дата_регистрации_противопоказания_или_отказа
		from KS_DDLControl.sl_onk_sl_b_prot as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_ksg_kpg_crit ( -- ТЧ Классификационные критерии
		id_up,
		crit_v024  -- Классификатор_классификационных_критериев
		)
		select
		v_new_id_sl,
		crit_v024  -- Классификатор_классификационных_критериев
		from KS_DDLControl.sl_ksg_kpg_crit as t
		where t.id_up = v_id_sl
		;
		insert into KS_DDLControl.sl_ksg_kpg_sl_koef ( -- ТЧ Коэффициенты сложности лечения пациента
		id_up,
		z_sl , -- Значение_коэффициента_сложности_лечения_пациента
		idsl  -- Номер_коэффициента_сложности_лечения_пациента
		)
		select
		v_new_id_sl,
		z_sl , -- Значение_коэффициента_сложности_лечения_пациента
		idsl  -- Номер_коэффициента_сложности_лечения_пациента
		from KS_DDLControl.sl_ksg_kpg_sl_koef as t
		where t.id_up = v_id_sl
		;

		insert into t_usl_onk
		select
		id
		from KS_DDLControl.sl_onk_sl_onk_usl as t
		where t.id_up = v_id_sl
		;
	
		while exists(select * from t_usl_onk) loop -- цикл по ТЧ Сведения об услуге при лечении онкологического заболевания.
			select id into v_id_usl_onk from t_usl_onk limit 1;
	
			insert into KS_DDLControl.sl_onk_sl_onk_usl ( 
			id_up,
			luch_tip_n017 , -- Тип_лучевой_терапии
			lek_tip_v_n016 , -- Цикл_лекарственной_терапии
			lek_tip_l_n015 , -- Линия_лекарственной_терапии
			hir_tip_n014 , -- Тип_хирургического_лечения
			usl_tip_n013 , -- Тип_услуги
			pptr  -- Признак_проведения_профилактики_тошноты_и_рвотного_рефлекса
			)
			select
			v_new_id_sl,
			luch_tip_n017 , -- Тип_лучевой_терапии
			lek_tip_v_n016 , -- Цикл_лекарственной_терапии
			lek_tip_l_n015 , -- Линия_лекарственной_терапии
			hir_tip_n014 , -- Тип_хирургического_лечения
			usl_tip_n013 , -- Тип_услуги
			pptr  -- Признак_проведения_профилактики_тошноты_и_рвотного_рефлекса
			from KS_DDLControl.sl_onk_sl_onk_usl as t
			where t.id = v_id_usl_onk
			returning id into v_new_id_usl_onk
			;
		
			insert into t_usl_onk1
			select
			id
			from KS_DDLControl.sl_onk_sl_onk_usl_lek_pr as t
			where t.id_up = v_id_usl_onk
			;
			while exists(select * from t_usl_onk1) loop -- цикл по ТЧ Сведения о введенном противоопухолевом лекарственном препарате
				select id into v_id_usl_onk1 from t_usl_onk limit 1;
			
				insert into KS_DDLControl.sl_onk_sl_onk_usl_lek_pr (
				id_up,
				regnum_n020 , -- Классификатор_лекарственных_препаратов,_применяемых_при_проведении_лекарственной_терапии
				code_sh_v024  -- Классификатор_классификационных_критериев
				)
				select
				v_id_usl_onk,
				regnum_n020 , -- Классификатор_лекарственных_препаратов,_применяемых_при_проведении_лекарственной_терапии
				code_sh_v024  -- Классификатор_классификационных_критериев
				from KS_DDLControl.sl_onk_sl_onk_usl_lek_pr as t
				where t.id = v_id_usl_onk1
				returning id into v_new_id_usl_onk1
				;
				insert into KS_DDLControl.sl_onk_sl_onk_usl_lek_pr_date_inj ( -- ТЧ Даты введения лекарственного препарата
				id_up,
				date_inj  -- Дата_введения_лекарственного_препарата
				)
				select
				v_new_id_usl_onk1,
				date_inj  -- Дата_введения_лекарственного_препарата
				from KS_DDLControl.sl_onk_sl_b_prot as t
				where t.id_up = v_id_usl_onk1			
				;
			
				delete from t_usl_onk1 where id = v_id_usl_onk1;
			END loop
			;
	
			delete from t_usl_onk where id = v_id_usl_onk;
		END loop
		;

		insert into t_usl
		select
		usl_spr -- спр. Реестр оказанных услуг
		from KS_DDLControl.sl_usl as t
		where t.id_up = v_id_sl
		;
		while exists(select * from t_usl) loop -- цикл по ТЧ Сведения об услуге при лечении онкологического заболевания.
			select id into v_id_usl from t_usl limit 1;

			insert into KS_DDLControl.usl ( -- Реестр оказанных услуг
			filename , -- Имя_файла
			idcase , -- Идентификатор_законченного_случая
			sl_id , -- Идентификатор_случая
			idserv , -- Идентификатор
			pacient , -- Сведения_о_пациенте
			lpu , -- МО
			profil , -- Профиль_медицинской_помощи
			vid_vme , -- Вид_медицинского_вмешательства
			det , -- Признак_детского_профиля
			date_in , -- Дата_начала_оказания_услуги
			date_out , -- Дата_окончания_оказания_услуги
			ds , -- Диагноз_(МКБ-10)
			code_usl , -- Номенклатура_работ_и_услуг_в_здравоохранении_(региональный_перечень)
			kol_usl , -- Количество_услуг_(кратность_услуги)
			tarif , -- Тариф
			sumv_usl , -- Стоимость_медицинской_услуги,_выставленная_к_оплате_(руб.)
			prvs  -- Специальность_медработника,_выполнившего_услугу
			)
			select 
			t_d.Имя_файла, 
			idcase , -- Идентификатор_законченного_случая
			sl_id , -- Идентификатор_случая
			idserv , -- Идентификатор
			pacient , -- Сведения_о_пациенте
			lpu , -- МО
			profil , -- Профиль_медицинской_помощи
			vid_vme , -- Вид_медицинского_вмешательства
			det , -- Признак_детского_профиля
			date_in , -- Дата_начала_оказания_услуги
			date_out , -- Дата_окончания_оказания_услуги
			ds , -- Диагноз_(МКБ-10)
			code_usl , -- Номенклатура_работ_и_услуг_в_здравоохранении_(региональный_перечень)
			kol_usl , -- Количество_услуг_(кратность_услуги)
			tarif , -- Тариф
			sumv_usl , -- Стоимость_медицинской_услуги,_выставленная_к_оплате_(руб.)
			prvs  -- Специальность_медработника,_выполнившего_услугу
			from t_d 
			inner join KS_DDLControl.usl on usl.id = v_id_usl
			returning id into v_new_id_usl 
			;
			
			-- добавляем в ТЧ Сведения о случае созданный элемент спр. Реестр оказанных услуг
			insert into	KS_DDLControl.sl_usl ( id_up, usl_spr ) values ( v_new_id_sl, v_new_id_usl ) 
			;

			delete from t_usl where id = v_id_usl;
		END loop
		;
		-- добавляем в ТЧ Сведения о случае созданный элемент спр. Реестр случаев 
		insert into	KS_DDLControl.z_sl_sl ( id_up, sl_spr ) values ( v_new_id_rzl, v_new_id_sl ) 
		;
	
		delete from t_rzl_sl where id = v_id_sl;
	END loop
	;

	-- получаем максимальный порядковый номер записи этой в ТЧ Записи текущего документа
	select max(n_zap) into v_num_rzl from KS_DDLControl.zl_list_zap where id_up = v_doc_id
	;
	-- добавляем в ТЧ Записи текущего документа созданный элемент спр. Реестр законченных случаев 
	insert into	KS_DDLControl.zl_list_zap ( 
	id_up, 
	pacient_spr , -- Сведения_о_пациенте_(идентифицированный)
	z_sl_spr  -- Реестр_законченных_случаев
	) 
	select
	v_doc_id , 
	ПАЦИЕНТ_ID,
	v_new_id_rzl 
	from t_inst
	where ЗСЛ_ID = v_id_rzl
	returning id into v_new_id_zap
	;
	-- пишем его в созданный элемент в поле Номер позиции записи
	update KS_DDLControl.zl_list_zap
	set n_zap = coalesce(v_num_rzl, 0) + 1
	where id = v_new_id_zap
	;
	update KS_DDLControl.z_sl
	set n_zap = coalesce(v_num_rzl, 0) + 1
	where id = v_new_id_rzl
	;

	delete from t_inst0 where ЗСЛ_ID = v_id_rzl;
END loop
;

drop table if exists KS_DDLControl.uk_3093;
perform dbo.cl_unique_trg (v_nentityid := 3093, v_protrid := NULL);
drop table if exists KS_DDLControl.uk_3136;
perform dbo.cl_unique_trg (v_nentityid := 3136, v_protrid := NULL);
drop table if exists KS_DDLControl.uk_3139;
perform dbo.cl_unique_trg (v_nentityid := 3139, v_protrid := NULL);


end
-- $$;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3107_3812_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- 9.1.	ДЕЙСТВИЕ ПРИ СОХРАНЕНИИ документа «Файлы реестров счетов по оплате медицинских услуг (R-файл и D-файл –  Исходящий МТР)»

declare
--v_doc_id integer;
v_Unit uuid;
v_old_cnt integer;
v_new_cnt integer;
begin 

	--return;
--v_doc_id := 100;

select dbo.sys_guid() 
into v_Unit;
-- select * from KS_DDLControl.d001_sp

v_old_cnt := 0;

create temporary table v_temp_d on commit drop as -- текущий документ
select 
filename as Имя_файла,
data_norm as Дата,
left(cast(data_norm as varchar(8)), 4) as Y,
f10_1.kod_tf as Кто,
f10_2.kod_tf as Кому
from KS_DDLControl.zl_list as d -- Сведения об оказанной МП / Файлы реестров счетов по оплате медицинских услуг (R-файл и D-файл –  Исходящий МТР)

inner join ks_ddlcontrol.o002 o1 ON o1.id = d.c_okato1_o002
inner join ks_ddlcontrol.f010_vers f10v1 ON f10v1.kod_okato = o1.id and coalesce(f10v1.datebeg,0) = (select max(coalesce(f10v1_v.datebeg,0)) from ks_ddlcontrol.f010_vers f10v1_v where (f10v1_v.id_up = f10v1.id_up and coalesce(f10v1_v.datebeg,0) <= d.data_norm))
inner join ks_ddlcontrol.f010 f10_1 ON f10_1.id = f10v1.id_up

inner join ks_ddlcontrol.o002 o2 ON o2.id = d.okato_oms_o002
inner join ks_ddlcontrol.f010_vers f10v2 ON f10v2.kod_okato = o2.id and coalesce(f10v2.datebeg,0) = (select max(coalesce(f10v2_v.datebeg,0)) from ks_ddlcontrol.f010_vers f10v2_v where (f10v2_v.id_up = f10v2.id_up and coalesce(f10v2_v.datebeg,0) <= d.data_norm))
inner join ks_ddlcontrol.f010 f10_2 ON f10_2.id = f10v2.id_up

where d.id = v_doc_id
;
if(exists(select * from v_temp_d where coalesce(Имя_файла,'') = '')) then

	create temporary table v_temp_exd on commit drop as -- существующие документы
	select 
	t.id,
	filename as Имя_файла,
	data_norm as Дата,
	cast(left(cast(data_norm as varchar(8)), 4) as int) as Y
	from v_temp_d as d
	inner join KS_DDLControl.zl_list as t on t.id <> v_doc_id and t.documentid=3812 and left(t.filename, 1) = 'R' and left(cast(t.data_norm as varchar(8)), 4) = d.Y
	;
	select count(id) into v_old_cnt from v_temp_exd
	; 
	
	update KS_DDLControl.zl_list
	set filename = 'R'||d.Кто||d.Кому||right(d.Y, 2)||right('0000'||cast((v_old_cnt+1) as varchar(10)), 4)
	from v_temp_d as d
	where KS_DDLControl.zl_list.id = v_doc_id 
	;

end if
;

insert into dbo.sys_values(rid, code, int) values(v_Unit, 'EntityId', 3107); -- id макета
insert into dbo.sys_values(rid, code, int) values(v_Unit, 'DocumentId', 3812); -- id документа/справочника
insert into dbo.sys_values(rid, code, int) values(v_Unit, 'ElementId', v_doc_id); -- id элемента

perform dbo.cl_unique_modify (v_rid := v_Unit);
delete from dbo.sys_values where rid = v_Unit;

end
--end $$;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3115_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- 10.1.	СКРИПТ ДЛЯ ЗАПОЛНЕНИЯ УНИКАЛЬНОГО НОМЕРА ОБРАЩЕНИЯ В ЭД «Регистрационно-контрольная карта»
--1.	Скрипт для заполнения атрибута «Шапка документа. Уникальный номер обращения»
--2.	Повесить процедуру на кнопку «Сохранить»
--3.	Используемый документ: МИД «Электронный журнал» / документ «Регистрационно-контрольная карта»

declare
--v_doc_id integer;

begin 
--v_doc_id := 247;

-- select * from KS_DDLControl.irp
--delete from KS_DDLControl.zl_list where left(filename, 1)='D'  and st_owner=92 

--drop table if exists KS_DDLControl.uk_3115;
--perform dbo.cl_unique_trg (v_nentityid := 3115, v_protrid := NULL);

create temporary table t_d on commit drop as -- текущий документ
select 
id,
n_irp,
'04_'||right(left(cast(date_create as varchar),8),6)||'_' as pref
from KS_DDLControl.irp as d -- МИД «Электронный журнал» / документ «Регистрационно-контрольная карта»
where d.id = v_doc_id
;
if exists (select * from t_d where n_irp is not null and not exists(select * from KS_DDLControl.irp as d where d.id <> v_doc_id and t_d.n_irp = d.n_irp)) then
	RAISE notice 'Уникальный номер обращения - уже заполнен';
	return;
end if
;

create temporary table t_inst on commit drop as -- исходные данные
select 
max(cast(right(coalesce(n_irp,'0'), 7) as int)) as num
from (select 1 as a) as a
left join KS_DDLControl.irp as d on d.id <> v_doc_id -- МИД «Электронный журнал» / документ «Регистрационно-контрольная карта»
;

update ks_ddlcontrol.irp	
set n_irp = pref || right('0000000'||cast(num+1 as varchar), 7)
from t_d, t_inst
where ks_ddlcontrol.irp.id = v_doc_id
;

drop table if exists KS_DDLControl.uk_3115;
perform dbo.cl_unique_trg (v_nentityid := 3115, v_protrid := NULL);

end 
--$$;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3150(v_nmode integer DEFAULT 0, v_table_name_result character varying DEFAULT ''::character varying, v_ckiguid character varying DEFAULT ''::character varying, "v_дата" text DEFAULT '20191201'::text, "v_снилс" text DEFAULT '123-123-123 33'::text, "v_статус" text DEFAULT '111'::text)
 RETURNS TABLE(t character varying, "врач" character varying, ord character varying, num character varying, "показатель" text, "значение" text)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

-- отчет "Решение ТФОМС о внесении в территориальный реестр ЭКМП"

declare
    v_sel text;
	v_seldel text;
   	v_sel1 text;
    v_temp_result varchar(254);

    v_table_сведения character varying default null;
    v_table_организация character varying default null;
    v_table_сертификат character varying default null;
    v_table_ученая_степень character varying default null;

	v_tmp_guid character varying default null;
	v_msg_text text;
	v_except_detail text;
	v_except_hint text;

	v_dty varchar(4);
	v_dt varchar(8);

begin

v_tmp_guid := replace(cast(dbo.sys_guid() as varchar(36)),'-','_');

v_table_сведения := concat('запрос_61527349_',v_tmp_guid);
v_table_организация := concat('запрос_62306346_',v_tmp_guid);
v_table_сертификат := concat('запрос_62914760_',v_tmp_guid);
v_table_ученая_степень := concat('запрос_65335923_',v_tmp_guid);

v_seldel := '
drop table if exists '||v_table_сведения||';
drop table if exists '||v_table_организация||';
drop table if exists '||v_table_сертификат||';
drop table if exists '||v_table_ученая_степень||';
';

begin 
  perform ks_ddlcontrol.sp_ds_query_6272 (v_nmode := v_nmode, v_table_name_result := v_table_сведения, v_ckiguid := v_ckiguid, v_Дата := v_дата, v_Врач := v_снилс, v_Статус := v_статус);
  perform ks_ddlcontrol.sp_ds_query_6273 (v_nmode := v_nmode, v_table_name_result := v_table_организация, v_ckiguid := v_ckiguid, v_Дата := v_дата, v_Врач := v_снилс, v_Статус := v_статус);
  perform ks_ddlcontrol.sp_ds_query_6274 (v_nmode := v_nmode, v_table_name_result := v_table_сертификат, v_ckiguid := v_ckiguid, v_Дата := v_дата, v_Врач := v_снилс, v_Статус := v_статус);
  perform ks_ddlcontrol.sp_ds_query_6275 (v_nmode := v_nmode, v_table_name_result := v_table_ученая_степень, v_ckiguid := v_ckiguid, v_Дата := v_дата, v_Врач := v_снилс, v_Статус := v_статус);
exception when others then
  	get stacked diagnostics v_msg_text = message_text,
                          v_except_detail = pg_exception_detail,
                          v_except_hint = pg_exception_hint;
	raise exception 'ошибка получения исходных данных: %', v_msg_text
      using hint = 'проверьте используемые запросы.';

end; 


v_temp_result := concat('temp_result_',v_tmp_guid);


v_sel := '

--drop table if exists '||v_temp_result||'; 
create temporary table '||v_temp_result||' (
	t varchar(100),
	врач varchar(100),
	ord varchar(100),
	num varchar(100),
	показатель text,
	значение text
) on commit drop;

 

create temporary table t_врачи on commit drop as 
select 
right(''00''||n, 2) as ord, * 
from (
	select distinct
	cast(dense_rank() over(partition by субъект_рф order by фамилия_эксперта, имя_эксперта) as varchar) as n,
	субъект_рф,
	снилс,
	coalesce(код_эксперта,'''') as код_эксперта,
	coalesce(фамилия_эксперта,'''')||coalesce('' ''||имя_эксперта,'''')||coalesce('' ''||отчество_эксперта,'''') as фио
	from '||v_table_сведения||'
) as s;

insert into '||v_temp_result||'
select 
''Заголовок'',
t.снилс,
t.ord,
'''',
t.субъект_рф,
''''
from t_врачи as t
order by t.ord
limit 1;

insert into '||v_temp_result||'
select 
''Подвал'',
t.снилс,
t.ord||''_99'',
'''',
t.субъект_рф,
''''
from t_врачи as t
order by t.ord desc
limit 1;

insert into '||v_temp_result||'
select distinct
''Простая'',
t.снилс,
t.ord||''_01'',
'''',
t.n||'') Включить ''||фио||'' в территориальный реестр эксперта качества медицинской помощи по ''||субъект_рф||'' с присвоением персонального идентификационного номера ''||код_эксперта||'' на основании предоставленных документов:'',
''''
from t_врачи as t;

insert into '||v_temp_result||'
select distinct
''ЗаголовокТ'',
t.снилс,
t.ord||''_02'',
''№'',
''Документ'',
''Вносимые данные''
from t_врачи as t;

insert into '||v_temp_result||'
select distinct
''СтрокаТ'',
t.снилс,
t.ord||''_1_1'',
''1'',
''Данные основного документа, удостоверяющего личность гражданина Российской Федерации на территории Российской Федерации'',
coalesce(серия_документа,'''')
	||coalesce('' ''||номер_документа,'''')
	||coalesce('', ''||кем_выдан_документ,'''')
	||coalesce('', ''||to_char(дата_выдачи,''DD.MM.YYYY''),'''')
	||coalesce('', ''||место_регистрации_документа,'''')
	||''.''
from t_врачи as t
left join '||v_table_сведения||' as s on t.снилс = s.снилс and s.тип_документа_код = ''1'';

insert into '||v_temp_result||'
select distinct
''СтрокаТ'',
t.снилс,
t.ord||''_1_2'',
''2'',
''Организация, представившая кандидатуру врача-специалиста (при наличии ходатайства)'',
организации
from t_врачи as t
left join (
	select
	снилс,
	string_agg(distinct
			coalesce(наименование_организации,'''')
			||coalesce('', ''||юридический_адрес,'''')
			||coalesce('', ''||фактический_адрес,'''')
			||''.''
		, chr(10)) as организации	
	from '||v_table_организация||' 
	group by 
	снилс
) as s on t.снилс = s.снилс;


insert into '||v_temp_result||'
select distinct
''СтрокаТ'',
t.снилс,
t.ord||''_1_3'',
''3'',
''Диплом о высшем медицинском образовании'',
coalesce(название_специальности,'''')
	||coalesce('', ''||серия_документа,'''')||coalesce('' ''||номер_документа,'''')
	||coalesce('', ''||кем_выдан_документ,'''')
	||coalesce('', ''||to_char(дата_выдачи,''DD.MM.YYYY''),'''')
	||coalesce('', ''||место_регистрации_документа,'''')
	||''.''
from t_врачи as t
left join '||v_table_сведения||' as s on t.снилс = s.снилс and s.тип_документа_код = ''2'';

insert into '||v_temp_result||'
select distinct
''СтрокаТ'',
t.снилс,
t.ord||''_1_4'',
''4'',
''Свидетельство(а) об аккредитации или сертификат(ы) специалиста'',
name
from t_врачи as t
left join (
	select
	снилс,
	string_agg(distinct
			coalesce(кем_выдан,'''')
			||coalesce('', ''||to_char(дата_выдачи,''DD.MM.YYYY''),'''')
			||coalesce('', ''||coalesce(название_специальности1,название_специальности2,''''),'''')
			||coalesce('', ''||to_char(срок_действия,''DD.MM.YYYY''),'''')
			||''.''
		, chr(10)) as name	
	from '||v_table_сертификат||' 
	group by 
	снилс
) as s on t.снилс = s.снилс;

insert into '||v_temp_result||'
select distinct
''СтрокаТ'',
t.снилс,
t.ord||''_1_5'',
''5'',
''Документ, подтверждающий наличие подготовки по вопросам экспертной деятельности в сфере обязательного медицинского страхования'',
coalesce(название_цикла,'''')
	||coalesce('', ''||to_char(дата_прохождения,''DD.MM.YYYY''),'''')
	||coalesce('', ''||cast(количество_часов as varchar),'''')
	||coalesce('', ''||кем_выдан_документ,'''')
	||coalesce('', ''||to_char(дата_выдачи,''DD.MM.YYYY''),'''')
	||''.''
from t_врачи as t
left join '||v_table_сведения||' as s on t.снилс = s.снилс and s.тип_документа_код = ''3'';


insert into '||v_temp_result||'
select distinct
''СтрокаТ'',
t.снилс,
t.ord||''_1_6'',
''6'',
''Документ(ы), подтверждающий(ие) наличие квалификационной категории'',
name
from t_врачи as t
left join (
	select
	снилс,
	string_agg(distinct
			coalesce(квалификационная_категория,'''')
			||coalesce('', ''||to_char(дата_выдачи,''DD.MM.YYYY''),'''')
			||coalesce('', ''||номер_документа,'''')
			||coalesce('', ''||кем_выдан_документ,'''')
			||''.''
		, chr(10)) as name	
	from '||v_table_сведения||' as s
	where s.тип_документа_код = ''4''
	group by 
	снилс
) as s on t.снилс = s.снилс;

insert into '||v_temp_result||'
select distinct
''СтрокаТ'',
t.снилс,
t.ord||''_1_7'',
''7'',
''Документ, подтверждающий наличие подготовки по вопросам экспертной деятельности в сфере обязательного медицинского страхования'',
name
from t_врачи as t
left join (
	select
	снилс,
	string_agg(distinct
			coalesce(степень,'''')
			||coalesce('', ''||to_char(дата_присвоения,''DD.MM.YYYY''),'''')
			||coalesce('', ''||кем_присвоена,'''')
			||''.''
		, chr(10)) as name	
	from '||v_table_ученая_степень||' as s
	group by 
	снилс
) as s on t.снилс = s.снилс;

insert into '||v_temp_result||'
select distinct
''СтрокаТ'',
t.снилс,
t.ord||''_1_8'',
''8'',
''Выписка из трудовой книжки'',
name
from t_врачи as t
left join (
	select
	снилс,
	string_agg(distinct
			coalesce(место_работы,'''')
			||coalesce('', ''||юридический_адрес,'''')
			||coalesce('', ''||фактический_адрес,'''')
			||coalesce('', ''||телефон_1,'''')
			||coalesce('', ''||телефон_2,'''')
			||coalesce('', ''||должность,'''')
			||''.''
		, chr(10)) as name	
	from '||v_table_сертификат||' as s
	group by 
	снилс
) as s on t.снилс = s.снилс;


';

begin 
	execute (v_sel);
exception when others then
  	get stacked diagnostics v_msg_text = message_text,
                          v_except_detail = pg_exception_detail,
                          v_except_hint = pg_exception_hint;
	execute (v_seldel);
	raise exception 'ошибка формирования результата отчета: %', coalesce(v_msg_text,'')
      using hint = v_sel;

end; 

execute (v_seldel);

-- финальный процесс
if coalesce(v_table_name_result,'') <> '' then

      v_sel := concat('drop table if exists ', v_table_name_result,';create temporary table ', v_table_name_result , ' on commit drop as select  * from ' , v_temp_result , ' order by ord ');
      execute v_sel;

else

      v_sel := concat('select  * from ', v_temp_result, ' order by ord  ');
      return query execute v_sel;

end if;

end;

$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3157_save_(v_spr_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

begin 
-- при сохранении справочника из макета Сотрудники
-- заполнения атрибутов Код организации, Наименование организации

update KS_DDLControl.sotr
set org_code = inst.code, org_name = inst.name
from KS_DDLControl.sotr as spr 
inner join (
	select 
	sotr.id,
	coalesce(tfoms_tf.kod_tf, ersmo.smocod, ermo.mcod, oiv.code) as code,
	coalesce(tfoms_d.name_tfp  , ersmo_d.nam_smop, ermo_d.nam_mop, oiv_v.name) as name
	from KS_DDLControl.sotr as sotr
	left join KS_DDLControl.f001_r as tfoms on tfoms.id = sotr.tfoms
	left join KS_DDLControl.f010 as tfoms_tf on tfoms_tf.id = tfoms.tf_kod
	left join KS_DDLControl.f001_r_vers as tfoms_v on tfoms_v.id_up=tfoms.id 
		and tfoms_v.date = (select max(date) from KS_DDLControl.f001_r_vers as tfoms_v0 where tfoms_v0.id_up=tfoms.id)
	left join KS_DDLControl.f001_d as tfoms_d on tfoms_d.id=tfoms_v.f001_d 
	left join KS_DDLControl.f002_r as ersmo on ersmo.id = sotr.smo
	left join KS_DDLControl.f002_r_vers as ersmo_v on ersmo_v.id_up=ersmo.id 
		and ersmo_v.date = (select max(date) from KS_DDLControl.f002_r_vers as ersmo_v0 where ersmo_v0.id_up=ersmo_v.id_up)
	left join KS_DDLControl.f002_d as ersmo_d on ersmo_d.id=ersmo_v.f002_d 
	left join KS_DDLControl.f003_r as ermo on ermo.id = sotr.mo
	left join KS_DDLControl.f003_r_vers as ermo_v on ermo_v.id_up=ermo.id 
		and ermo_v.date = (select max(date) from KS_DDLControl.f003_r_vers as ermo_v0 where ermo_v0.id_up=ermo_v.id_up)
	left join KS_DDLControl.f003_d as ermo_d on ermo_d.id=ermo_v.f003_d 
	left join KS_DDLControl.oiv as oiv on oiv.id = sotr.oiv
	left join KS_DDLControl.oiv_vers as oiv_v on oiv_v.id_up=oiv.id 
		and oiv_v.datebeg = (select max(datebeg) from KS_DDLControl.oiv_vers as oiv_v0 where oiv_v0.id_up=oiv_v.id)
	where sotr.id = v_spr_id
) as inst on inst.id = spr.id  
where KS_DDLControl.sotr.id = v_spr_id and spr.id = v_spr_id;

end;

$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3190_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

declare
v_Unit uuid;

begin 
	
	select dbo.sys_guid() 
	into v_Unit;
	
	-- Добавляем элементы справочника которых нет
	insert into ks_ddlcontrol.ts006_contents 
	(id_up, guid, unit, 
	ind
	)
	select v_doc_id, dbo.sys_guid(), v_Unit,
	spr.id 
	from ks_ddlcontrol.tsp006 spr
	where class='Расчетный' and not exists (
	select * 
	from ks_ddlcontrol.ts006_contents doc  
	where doc.id_up = v_doc_id and doc.ind = spr.id
	);
	
	create temporary table tarif on commit drop as
	select 
	case when sum(case when ord = 2 then fare else 0 end)=0 then 0 else round(sum(case when ord = 1 then fare else 0 end) / sum(case when ord = 2 then fare else 0 end) * 1000, 2) end tarif_3,
	case when sum(case when ord = 2 then fare else 0 end)=0 then 0 else round((sum(case when ord = 1 then fare else 0 end) / sum(case when ord = 2 then fare else 0 end) * 1000)*99/100, 2) end tarif_4,
	case when sum(case when ord = 2 then fare else 0 end)=0 then 0 else round((sum(case when ord = 1 then fare else 0 end) / sum(case when ord = 2 then fare else 0 end) * 1000)*99/100/12, 2) end tarif_5,
	case when sum(case when ord = 2 then fare else 0 end)=0 then 0 else round(sum(case when ord = 1 then fare else 0 end) / sum(case when ord = 2 then fare else 0 end) * 1000, 2) end tarif_6
	from ks_ddlcontrol.ts006_contents doc
	inner join ks_ddlcontrol.tsp006_vers spr on doc.ind = spr.id_up and spr.ord in (1,2)
	where doc.id_up = v_doc_id 
	;

	--	Записать в атрибут Тариф, где Порядок = 3 значение рассчитанное по формуле: (Тариф, где Порядок 1 разделить на Тариф, где Порядок 2 и умножить на 1000). 
	update  KS_DDLControl.ts006_contents target
	set fare =  tarif_3
	from ks_ddlcontrol.ts006_contents doc
	inner join ks_ddlcontrol.tsp006_vers spr on doc.ind = spr.id_up and spr.ord =3
	inner join tarif t on 1=1
	where doc.id_up = v_doc_id and target.id = doc.id
	;

	--	Записать в атрибут Тариф, где Порядок = 4 значение рассчитанное по формуле: (Тариф, где Порядок =3 умножить на 99 и разделить на 100 
	update  KS_DDLControl.ts006_contents target
	set fare =  tarif_4
	from ks_ddlcontrol.ts006_contents doc
	inner join ks_ddlcontrol.tsp006_vers spr on doc.ind = spr.id_up and spr.ord =4
	inner join tarif t on 1=1
	where doc.id_up = v_doc_id and target.id = doc.id
	;

	-- Записать в атрибут Тариф, где Порядок = 5 значение рассчитанное по формуле: (Тариф, где Порядок =4 разделить на 12)
	update  KS_DDLControl.ts006_contents target
	set fare =  tarif_5
	from ks_ddlcontrol.ts006_contents doc
	inner join ks_ddlcontrol.tsp006_vers spr on doc.ind = spr.id_up and spr.ord =5
	inner join tarif t on 1=1
	where doc.id_up = v_doc_id and target.id = doc.id
	;

	-- Записать в атрибут Тариф, где Порядок = 6 значение рассчитанное по формуле: (Тариф, где Порядок =1 разделить на 2)
	update  KS_DDLControl.ts006_contents target
	set fare =  tarif_6
	from ks_ddlcontrol.ts006_contents doc
	inner join ks_ddlcontrol.tsp006_vers spr on doc.ind = spr.id_up and spr.ord =6
	inner join tarif t on 1=1
	where doc.id_up = v_doc_id and target.id = doc.id
	;

end;

$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3200_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

declare
v_Unit uuid;

begin 
	
-- ТЧ.Медицинские организации.ТЧ.ТЧ Перечень ФП и ФАП – Перечень ФП
	
update KS_DDLControl.ts008_mo_sp
--10.	Вычислить значения атрибутов Перечень ФП.Годовой размер финансового обеспечения по следующему алгоритму: 
--если Перечень ФП.Численность, чел >=100 и <900 и Перечень ФП.по приказу Мнздрава = Да , 
--то Шапка.Коэф100900*ТЧ Медицинские организации.Коэффициент(из вышестоящей ТЧ); 
--если Перечень ФП.Численность, чел >=900 и <1500 и Перечень ФП.по приказу Мнздрава = Да, 
--то Шапка.Коэф9001500*ТЧ Медицинские организации.Коэффициент(из вышестоящей ТЧ); 
--если Перечень ФП.Численность, чел >=1500 и <2000 и Перечень ФП.по приказу Мнздрава = Да, 
--то Шапка.Коэф15002000*ТЧ Медицинские организации.Коэффициент(из вышестоящей ТЧ), 
--иначе ничего не заполнять
set sumyear = d.sumyear
from (
	select 
	ts_mo_sp.id,
	case when 100 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 900 and coalesce(ts_mo_sp.ord,0) = 1 then doc.koef2 
		when 900 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 1500 and coalesce(ts_mo_sp.ord,0) = 1 then doc.koef3
		when 1500 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 2000 and coalesce(ts_mo_sp.ord,0) = 1 then doc.koef4
		else 0 end * ts_mo.koefmo as sumyear
	from KS_DDLControl.ts008 as doc -- ЭД Приложение №8 к тарифному соглашению
	inner join KS_DDLControl.ts008_mo as ts_mo -- ТЧ.Медицинские организации
		on doc.id = ts_mo.id_up
	inner join KS_DDLControl.ts008_mo_sp as ts_mo_sp -- ТЧ.Перечень ФП и ФАП
		on ts_mo.id = ts_mo_sp.id_up
	where doc.id = v_doc_id
) as d 
where KS_DDLControl.ts008_mo_sp.id = d.id 
	and 100 <= KS_DDLControl.ts008_mo_sp.ppl and KS_DDLControl.ts008_mo_sp.ppl < 2000 ;


update ks_ddlcontrol.ts008
set sum100 = d.sum100,
sum899 = d.sum899,
ord899 = d.ord899,
sum1500 = d.sum1500,
ord1500 = d.ord1500,
sum2000 = d.sum2000,
ord2000 = d.ord2000,
sumover2000 = d.sumover2000,
sum1 = d.sum1,
sum2 = d.sum2,
sum3 = d.sum3
from (
	select 
	--2.	Вычислить и записать значение атрибута Шапка.Всегодо100 по формуле: 
	--Посчитать количество всех строк в Перечень ФП, где Перечень ФП.Численность, чел. < 100
	sum(case when ts_mo_sp.ppl < 100 then 1 else 0 end) as sum100,
	--3.	Вычислить и записать значение атрибута Шапка.Всегодо100899 по формуле: 
	--Посчитать количество всех строк в Перечень ФП, где Перечень ФП.Численность, чел. >= 100 и < 900
	sum(case when 100 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 900 then 1 else 0 end) as sum899,
	--4.	Вычислить и записать значение атрибута Шапка.Поприказу100899 по формуле: 
	--Посчитать количество всех строк в Перечень ФП, где Перечень ФП.Численность, чел. >= 100 и < 900 и Перечень ФП.по приказу Минздрава = Да
	sum(case when 100 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 900 and coalesce(ts_mo_sp.ord, 0)=1 then 1 else 0 end) as ord899,
	--5.	Вычислить и записать значение атрибута Шапка.Всегодо9001500 по формуле: 
	--Посчитать количество всех строк в Перечень ФП, где Перечень ФП.Численность, чел. >= 900 и < 1500
	sum(case when 900 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 1500 then 1 else 0 end) as sum1500,
	--6.	Вычислить и записать значение атрибута Шапка.Поприказу100899 по формуле: 
	--Посчитать количество всех строк в Перечень ФП, где Перечень ФП.Численность, чел. >= 900 и < 1500 и Перечень ФП.по приказу Минздрава = Да
	sum(case when 900 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 1500 and coalesce(ts_mo_sp.ord, 0)=1 then 1 else 0 end) as ord1500,
	--7.	Вычислить и записать значение атрибута Шапка.Всегодо15002000 по формуле: 
	--Посчитать количество всех строк в Перечень ФП, где Перечень ФП.Численность, чел. >= 1500 и < 2000
	sum(case when 1500 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 2000 then 1 else 0 end) as sum2000,
	--8.	Вычислить и записать значение атрибута Шапка.Поприказу15002000 по формуле: 
	--Посчитать количество всех строк в Перечень ФП, где Перечень ФП.Численность, чел. >= 1500 и < 2000 и Перечень ФП.по приказу Минздрава = Да
	sum(case when 1500 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 2000 and coalesce(ts_mo_sp.ord, 0)=1 then 1 else 0 end) as ord2000,
	--9.	Вычислить и записать значение атрибута Шапка.Всегодо15002000 по формуле: 
	--Посчитать количество всех строк в Перечень ФП, где Перечень ФП.Численность, чел. >= 2000
	sum(case when ts_mo_sp.ppl >= 2000 then 1 else 0 end) as sumover2000,
	--11.	Вычислить и заполнить значение атрибута Шапка.Размер финобеспечения 1 по формуле: 
	--сложить все суммы из Перечень ФП.Годовой размер финансового обеспечения, 
	--где  (Вычисление производить после расчета п.10) Перечень ФП.Численность, чел >0 и <900
	sum(case when 0 < ts_mo_sp.ppl and ts_mo_sp.ppl < 900 then ts_mo_sp.sumyear else 0 end) as sum1,
	--12.	Вычислить и заполнить значение атрибута Шапка.Размер финобеспечения 2 по формуле: 
	--сложить все суммы из Перечень ФП.Годовой размер финансового обеспечения., где Перечень ФП.Численность, чел >=900 и <1500
	sum(case when 900 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 1500 then ts_mo_sp.sumyear else 0 end) as sum2,
	--13.	Вычислить и заполнить значение атрибута Шапка.Размер финобеспечения 3 по формуле: 
	--сложить все суммы из Перечень ФП.Годовой размер финансового обеспечения., где Перечень ФП.Численность, чел >=1500 и <2000
	sum(case when 1500 <= ts_mo_sp.ppl and ts_mo_sp.ppl < 2000 then ts_mo_sp.sumyear else 0 end) as sum3
	from ks_ddlcontrol.ts008 as doc -- ЭД Приложение №8 к тарифному соглашению
	inner join ks_ddlcontrol.ts008_mo as ts_mo -- ТЧ.Медицинские организации
		on doc.id = ts_mo.id_up
	inner join ks_ddlcontrol.ts008_mo_sp as ts_mo_sp -- ТЧ.Перечень ФП и ФАП
		on ts_mo.id = ts_mo_sp.id_up
	where doc.id = v_doc_id
) as d
where ks_ddlcontrol.ts008.id = v_doc_id ;


end;

$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3216_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

begin 
-- при сохранении документа Приложение №5 к тарифному соглашению
-- заполнения атрибутов ДПН1 , ДПН2 

update KS_DDLControl.ts005_mo_v006
set dpn1 =
	tspv.int -- Значение интегрированного коэфициента
	 * base.basekoef -- Номатив.Значение
from KS_DDLControl.ts005 as d -- документ Приложение №5 к тарифному соглашению
inner join KS_DDLControl.ts005_mo as mo on d.id = mo.id_up -- ТЧ Медицинские организации
inner join KS_DDLControl.ts005_mo_v006 as mo_v006 on mo.id = mo_v006.id_up -- ТЧ Коэффициенты в разрезе V006
inner join KS_DDLControl.ts005_base as base on d.id = base.id_up -- ТЧ Базовый подушевой норматив
inner join KS_DDLControl.tsp012 as tsp on tsp.id = mo_v006.intkoef -- спр. Значения интегрированных коэффициентов по уровням МО
-- Номатив. Классификатор условий оказания медицинской помощи (UslMp) = Коэф006.Значения инткоэффициентов.Условия оказания медпомощи
	and base.cl_138593 = tsp.v006  
inner join KS_DDLControl.tsp012_vers as tspv on tspv.id_up = tsp.id
	and coalesce(tspv.datebeg,0) = (select max(coalesce(tspv0.datebeg,0)) from KS_DDLControl.tsp012_vers as tspv0 where tspv0.id_up = tspv.id_up) 
where d.id = v_doc_id and KS_DDLControl.ts005_mo_v006.id = mo_v006.id;


end;

$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3300_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

declare
v_Unit uuid;
begin 

delete from ks_ddlcontrol.ds_3300_21203 t 
using ks_ddlcontrol.tp005_contents t2
left join ks_ddlcontrol.tp005 t3 on id_up=t2.id and t2.id_up=t3.id
where t.id_up=t2.id and t3.id=v_doc_id and t.id in (
select 
	t_l21198_l21203.id as таблица_значения_id
 from 
	(((((ks_ddlcontrol.tp005 t
		LEFT OUTER JOIN ks_ddlcontrol.spr_year t_139029 ON (t.year = t_139029.id))
		LEFT OUTER JOIN ks_ddlcontrol.tp005_contents t_l21198 ON (t.id = t_l21198.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.tpp004 t_l21198_139032 ON (t_l21198.tpp004 = t_l21198_139032.id))
		LEFT OUTER JOIN ks_ddlcontrol.ds_3300_21203 t_l21198_l21203 ON (t_l21198.id = t_l21198_l21203.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.spr_year t_l21198_l21203_139033 ON (t_l21198_l21203.year2 = t_l21198_l21203_139033.id))
where 
	t.documentid IN (3914) and t.id=v_doc_id and not(t_139029.year=t_l21198_l21203_139033.year or (t_139029.year+1)=t_l21198_l21203_139033.year or (t_139029.year+2)=t_l21198_l21203_139033.year)
	);
end;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3348_save_(v_spr_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

begin 
-- при сохранении справочника из макета Сотрудники
-- заполнения атрибутов Код организации, Наименование организации

update ks_ddlcontrol.version t0
set exp_ei=t3.name
from (
Select t1.id, t1.id_up, t2.name from ks_ddlcontrol.version t1
left join ks_ddlcontrol.tsp010_vers t2 on t1.ei=t2.id
where t1.exp_ei is null and not(t1.ei is null) and t1.id_up = v_spr_id
) as t3
where t0.id = t3.id and t0.id_up=t3.id_up;
end;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3381_3989_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- 7.1.	 Скрипт для заполнения атрибутов «ТЧ Запись. Карточка профилактического мероприятия. ТЧ История прохождения» в ЭД «Списки на диспансеризацию/профилактические осмотры»

declare
--v_doc_id integer;
v_Unit uuid;
v_old_cnt integer;
v_new_cnt integer;
begin 

--v_doc_id := 34;

select dbo.sys_guid() 
into v_Unit;
-- select * from KS_DDLControl.d001_sp

create temporary table v_temp_sp on commit drop as
select -- МИД «Списки профилактических мероприятий» / документ «Списки на диспансеризацию/профилактические осмотры».
sp.id,
sp.code_mo,
y.year,
sp_pac.id as sp_pac_id, 
k.id as karta_id,
k.i_pac -- Карта. ИД ЗЛ
from KS_DDLControl.d001_sp as sp 
inner join KS_DDLControl.d001_sp_pac as sp_pac on -- ТЧ Запись
	sp_pac.id_up = sp.id
inner join KS_DDLControl.spr_year as y on y.id = sp.year
inner join KS_DDLControl.d001_pac as k on -- Карточка профилактического мероприятия (Карта)
	sp_pac.karta = k.id
where sp.id = v_doc_id
;

-- (Пациент + Тип диспансеризации + Дата оказания МП + 
--Результат диспансеризации + Признак отказа + 
--Вид назначения + Диспансерное наблюдение + Злокачественное новообразование)
create temporary table v_temp_sp_visit on commit drop as 
select -- текущий документ. ТЧ Запись.Карточка профилактического мероприятия. ТЧ История прохождения
sp.karta_id,
visit.id as visit_id,
sp.i_pac, -- Пациент (Карта. ИД ЗЛ)
visit.tdisp, -- Тип диспансеризации (Классификатор типов диспансеризации (DispT))
visit.date_fact, -- Дата_оказания_МП
visit.rezdisp, -- Результат диспансеризации (Классификатор_результатов_диспансеризации_DispR)
p_otk, -- Признак отказа
naz_r_rs009, -- Вид назначения
pr_d_n_rs015, -- Диспансерное наблюдение
opyh -- Злокачественное новообразование
from KS_DDLControl.d001_visit as visit
inner join v_temp_sp as sp on sp.karta_id = visit.id_up;

-- ВНИМАНИЕ! данная таблица далее будет очищаться и перезаполняться
select count(*) into v_old_cnt from v_temp_sp_visit;

create temporary table v_temp_zl_list on commit drop as
select -- МИД «Сведения об оказанной МП» / документ «Сведения об оказанной медицинской помощи (X файл)»
d.id,
data_norm,
code_mo_f003, -- спр. МО
coalesce(plat_f002,0) as plat_f002, --спр. СМО (Плательщик)
disp_v016, -- спр. Типы диспансеризации
d.year,
d.month
from KS_DDLControl.zl_list as d
--Код МО = Текущий документ. Код МО
--Отчетный год = Текущий документ. Год
inner join (
	select distinct
	code_mo, year
	from v_temp_sp 
) as s on d.code_mo_f003 = s.code_mo and d.year = s.year
--Статус. Приоритет > 1
inner join KS_DDLControl.cl_958_3060 as stat on d.status = stat.id and stat.at_3067 > 1
where d.documentid = 3734;

delete from v_temp_zl_list	where data_norm <> (
	select max(data_norm) from v_temp_zl_list as t0
	where v_temp_zl_list.code_mo_f003 = t0.code_mo_f003 and 
		v_temp_zl_list.year = t0.year and
		v_temp_zl_list.month = t0.month and
		v_temp_zl_list.plat_f002 = t0.plat_f002 and 
		v_temp_zl_list.disp_v016 = t0.disp_v016  
);

create temporary table v_temp_zl_list_zap on commit drop as
select -- Сведения об оказанной медицинской помощи. ТЧ Записи
pacient.id_pac, -- Пациент (Сведения о пациенте (идентифицированный). Код записи о пациенте )
zl_list.disp_v016, -- Тип диспансеризации
sl.date_z_2 as date_fact, -- Дата_окончания_лечения
sl.rslt_d_v017 as rezdisp, -- Результат_диспансеризации
sl.p_otk, -- Признак_отказа
sl.id as sl_id
from KS_DDLControl.zl_list_zap as zap
inner join v_temp_zl_list as zl_list on zap.id_up = zl_list.id
inner join KS_DDLControl.tbl_pacient as pacient on zap.pacient_spr = pacient.id
inner join KS_DDLControl.z_sl as sl on sl.id = zap.z_sl_spr -- Реестр законченных случаев
;

create temporary table v_temp_zl_list_naz on commit drop as
select distinct -- Сведения об оказанной медицинской помощи. ТЧ Записи. Реестр законченных случаев. 
		-- ТЧ Сведения о случае. Реестр случаев. ТЧ Назначения
zap.sl_id,
naz.naz_r_rs009 as Вид_назначения
from KS_DDLControl.z_sl_sl as sl_sl
inner join v_temp_zl_list_zap as zap on zap.sl_id = sl_sl.id_up
inner join KS_DDLControl.sl as rsl on rsl.id = sl_sl.sl_spr -- Реестр случаев
inner join KS_DDLControl.sl_naz as naz on rsl.id = naz.id_up -- ТЧ Назначения
;

create temporary table v_temp_zl_list_ds on commit drop as
select -- Сведения об оказанной медицинской помощи. ТЧ Записи. Реестр законченных случаев. 
		-- ТЧ Сведения о случае. Реестр случаев. ТЧ ТЧ Сопутствующие заболевания (X-файл)
zap.sl_id,
ds2_n.pr_ds2_n_rs015 as Диспансерное_наблюдение
from KS_DDLControl.z_sl_sl as sl_sl
inner join v_temp_zl_list_zap as zap on zap.sl_id = sl_sl.id_up
inner join KS_DDLControl.sl as rsl on rsl.id = sl_sl.sl_spr -- Реестр случаев
inner join KS_DDLControl.sl_ds2_n as ds2_n on rsl.id = ds2_n.id_up -- ТЧ Сопутствующие заболевания (X-файл)
;
create temporary table v_temp_zl_list_rsl on commit drop as
select -- Сведения об оказанной медицинской помощи. ТЧ Записи. Реестр законченных случаев. 
		-- ТЧ Сведения о случае. Реестр случаев
zap.sl_id,
rsl.ds_onk_rs008 as Признак_злокачественное
from KS_DDLControl.z_sl_sl as sl_sl
inner join v_temp_zl_list_zap as zap on zap.sl_id = sl_sl.id_up
inner join KS_DDLControl.sl as rsl on rsl.id = sl_sl.sl_spr -- Реестр случаев
;
--Для каждой выбранной строки из ТЧ Записи документа «Сведения об оказанной медицинской помощи (X файл)» выполнить действия:
--Проверить, есть ли такая строка в текущем документе в ТЧ Запись. Карточка профилактического мероприятия. ТЧ История прохождения:

--- если такой строки нет в текущем документе, то необходимо ее добавить в текущий документ в ТЧ Запись. Карточка профилактического мероприятия. ТЧ История прохождения

create temporary table v_temp_insert on commit drop as
select distinct 
sp.karta_id,
zap.*
from v_temp_zl_list_zap as zap
inner join v_temp_sp as sp on sp.i_pac =  zap.id_pac
where not exists (
		select
		*
		from v_temp_sp_visit as visit -- текущий документ
		where visit.i_pac = zap.id_pac 
			and visit.tdisp	= zap.disp_v016 
			--and visit.date_fact	= zap.date_fact
	) 
	and zap.date_fact is not null;

insert into KS_DDLControl.d001_visit (
unit,
guid,
id_up,
tdisp,
date_fact
)
select 
v_Unit,
dbo.sys_guid(),
ins.karta_id,
ins.disp_v016,
ins.date_fact
from v_temp_insert as ins;

-- перезаполняем данные ТЧ История прохождения карточек из текущего документа
delete from v_temp_sp_visit;
insert into v_temp_sp_visit 
select -- текущий документ. ТЧ Запись.Карточка профилактического мероприятия. ТЧ История прохождения
sp.karta_id,
visit.id as visit_id,
sp.i_pac, -- Пациент (Карта. ИД ЗЛ)
visit.tdisp, -- Тип диспансеризации (Классификатор типов диспансеризации (DispT))
visit.date_fact, -- Дата_оказания_МП
visit.rezdisp, -- Результат диспансеризации (Классификатор_результатов_диспансеризации_DispR)
p_otk, -- Признак отказа
naz_r_rs009, -- Вид назначения
pr_d_n_rs015, -- Диспансерное наблюдение
opyh -- Злокачественное новообразование
from KS_DDLControl.d001_visit as visit
inner join v_temp_sp as sp on sp.karta_id = visit.id_up;
--select * from  KS_DDLControl.d001_visit where id_up = 49

select count(*) into v_new_cnt from v_temp_sp_visit;
raise notice 'v_old_cnt = %, v_new_cnt = %', v_old_cnt, v_new_cnt;
-- если такая строка есть, то обновить данные 
--в полях в ТЧ Запись. Карточка профилактического мероприятия. ТЧ История прохождения
create temporary table v_temp_update_data on commit drop as
select distinct 
visit.karta_id,
visit.visit_id,
zap.*
from v_temp_zl_list_zap as zap
inner join v_temp_sp_visit as visit on -- текущий документ
			visit.i_pac = zap.id_pac 
			and visit.tdisp	= zap.disp_v016 
			--and visit.date_fact	= zap.date_fact
;

update KS_DDLControl.d001_visit
set 
date_fact = t.date_fact,
rezdisp = t.rezdisp,
p_otk = t.p_otk
from KS_DDLControl.d001_visit as u
inner join v_temp_update_data as t on u.id = t.visit_id
where u.id = KS_DDLControl.d001_visit.id;

update KS_DDLControl.d001_visit
set 
cons = 1 -- Направлен на консультацию
from KS_DDLControl.d001_visit as u
inner join v_temp_update_data as t on u.id = t.visit_id
inner join v_temp_zl_list_naz as naz on naz.Вид_назначения in (1,2) and naz.sl_id = t.sl_id
where u.id = KS_DDLControl.d001_visit.id;

update KS_DDLControl.d001_visit
set 
obsl = 1 -- Направлен на обследование
from KS_DDLControl.d001_visit as u
inner join v_temp_update_data as t on u.id = t.visit_id
inner join v_temp_zl_list_naz as naz on naz.Вид_назначения = 3 and naz.sl_id = t.sl_id
where u.id = KS_DDLControl.d001_visit.id;

update KS_DDLControl.d001_visit
set 
stac = 1 -- Направлен в дневной стационар
from KS_DDLControl.d001_visit as u
inner join v_temp_update_data as t on u.id = t.visit_id
inner join v_temp_zl_list_naz as naz on naz.Вид_назначения = 4 and naz.sl_id = t.sl_id
where u.id = KS_DDLControl.d001_visit.id;

update KS_DDLControl.d001_visit
set 
gosp = 1 -- Направлен на госпитализацию
from KS_DDLControl.d001_visit as u
inner join v_temp_update_data as t on u.id = t.visit_id
inner join v_temp_zl_list_naz as naz on naz.Вид_назначения = 5 and naz.sl_id = t.sl_id
where u.id = KS_DDLControl.d001_visit.id;

update KS_DDLControl.d001_visit
set 
reab = 1 -- Направлен в реабилитационное отделение
from KS_DDLControl.d001_visit as u
inner join v_temp_update_data as t on u.id = t.visit_id
inner join v_temp_zl_list_naz as naz on naz.Вид_назначения = 6 and naz.sl_id = t.sl_id
where u.id = KS_DDLControl.d001_visit.id;

update KS_DDLControl.d001_visit
set 
disp = 1 -- Установлено диспансерное наблюдение
from KS_DDLControl.d001_visit as u
inner join v_temp_update_data as t on u.id = t.visit_id
inner join v_temp_zl_list_ds as ds on ds.Диспансерное_наблюдение in (1,2) and ds.sl_id = t.sl_id
where u.id = KS_DDLControl.d001_visit.id;

update KS_DDLControl.d001_visit
set 
opyh = 1 -- Злокачественное новообразование
from KS_DDLControl.d001_visit as u
inner join v_temp_update_data as t on u.id = t.visit_id
inner join v_temp_zl_list_rsl as rsl on rsl.Признак_злокачественное = 2 and rsl.sl_id = t.sl_id
where u.id = KS_DDLControl.d001_visit.id;

-- если в текущем документе есть строка и по ней заполнен атрибут «Дата оказания МП», 
-- которой нет в найденном документе «Сведения об оказанной медицинской помощи (X файл)», 
-- то очистить атрибуты данной строки (кроме Тип диспансеризации)
create temporary table v_temp_erase_data on commit drop as
select distinct 
visit.visit_id
from v_temp_sp_visit as visit -- текущий документ
where not exists (
		select
		*
		from v_temp_zl_list_zap as zap
		where visit.i_pac = zap.id_pac 
			and visit.tdisp	= zap.disp_v016 
			and visit.date_fact	= zap.date_fact
	);
update KS_DDLControl.d001_visit
set 
date_fact = null,
rezdisp = null,
p_otk = null,
cons = null,
obsl = null,
stac = null,
gosp = null,
reab = null,
opyh = null,
disp = null
from KS_DDLControl.d001_visit as u
inner join v_temp_erase_data as t on u.id = t.visit_id
where u.id = KS_DDLControl.d001_visit.id;
end

/*
$$;
select * from v_temp_sp;
select * from v_temp_insert;
select * from v_temp_zl_list;
*/
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3381_3991_fill_data_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- 7.2 ДЕЙСТВИЕ «Заполнить исходными данными» для документа МИД «Списки профилактических мероприятий» / документ «Списки по диспансерному наблюдению». 

declare
--v_doc_id integer;
v_Unit uuid;
v_old_cnt integer;
v_new_cnt integer;

v_num_rzl integer;
v_new_id_zap integer;

v_id_rzl integer;
v_id_sl integer;
v_id_usl integer;
v_id_usl_onk integer;
v_id_usl_onk1 integer;
v_id_usl_onk2 integer;

v_new_id_rzl integer;
v_new_id_sl integer;
v_new_id_usl integer;
v_new_id_usl_onk integer;
v_new_id_usl_onk1 integer;
v_new_id_usl_onk2 integer;

begin 

--v_doc_id := 50;

select dbo.sys_guid() 
into v_Unit;
-- select * from KS_DDLControl.d001_sp where documentid = 3991  

v_old_cnt := 0;

create temporary table t_d on commit drop as -- текущий документ
select 
d.id,
d.code_mo as Код_МО,
y.year as Год,
k.id as k_id, -- Карточка_профилактического_мероприятия
kt.id as История_прохождения,
k.i_pac as Пациент, -- ИД_ЗЛ,
mv.kod as Плановый_период_посещения,
kt.place as Место_проведения_диспансерного_приема,
date_fact as Дата_оказания_МП,
rezult as Результат_обращения, -- Классификатор_результатов_обращения_за_медицинской_помощью,
mkb_fact as Основной_диагноз, -- Основной_диагноз_по_результатам_диспансерного_осмотра,
mo_fact as МО_по_результатам_диспансерного_осмотра
from KS_DDLControl.d001_sp as d -- МИД «Списки профилактических мероприятий» / документ «Списки по диспансерному наблюдению»
inner join ks_ddlcontrol.spr_year as y ON d.year = y.id -- Годы
inner join ks_ddlcontrol.d001_sp_pac as t on t.id_up = d.id -- текущий документ / ТЧ Запись
inner join ks_ddlcontrol.d001_pac as k on k.id = t.karta -- ТЧ Запись. Карточка_профилактического_мероприятия,
inner join ks_ddlcontrol.d001_visit as kt on kt.id_up = t.karta -- ТЧ Запись. Карточка профилактического мероприятия. ТЧ История прохождения
left join ks_ddlcontrol.tsp018 as mv on mv.id = kt.month_vis -- ТЧ История прохождения. Плановый период посещения
where d.documentid = 3991 and d.id = v_doc_id
;
create temporary table t_inst on commit drop as -- документы 
select 
d.id,
d.status as Статус,
d.filename as Имя_файла,
d.data_norm as Дата,
left(cast(d.data_norm as varchar(8)), 4) as Год,
d.year as Отчетный_год,
d.month as Отчетный_месяц,
d.code_mo_f003 as Код_МО, --спр_МО, 
plat_f002 as спр_СМО_Плательщик, -- Код СМО
zap.pacient_spr as ПАЦИЕНТ_ID, --Сведения_о_пациенте
pac.id_pac as Пациент,-- Код_записи_о_пациенте,
zap.z_sl_spr as Законченный_случай, -- Реестр_законченных_случаев
z_sl.date_z_2 as Дата_оказания_МП, --Дата_окончания_лечения
z_sl.rslt_v009 as Результат_обращения,
ds1_m001 as Диагноз_основной, --  Основной диагноз
sl_naz.naz_napr_mo_f003 as МО_по_результатам, -- МО_куда_оформлено_направление,
rs015.kod as Диспансерное_наблюдение
from KS_DDLControl.zl_list as d -- МИД «Сведения об оказанной МП» / документы
--																«Сведения об оказанной медицинской помощи (C файл)»
--																«Сведения об оказанной медицинской помощи (H файл)»
inner join ks_ddlcontrol.cl_958_3060 as st on st.id = d.status and st.at_3067 > 1 -- Статус. Приоритет > 1
inner join ks_ddlcontrol.zl_list_zap as zap on d.id = zap.id_up -- ТЧ Записи
left join ks_ddlcontrol.tbl_pacient as pac on pac.id = zap.pacient_spr -- Сведения о пациенте (идентифицированный)
left join ks_ddlcontrol.z_sl on z_sl.id = zap.z_sl_spr -- Реестр_законченных_случаев
left join ks_ddlcontrol.z_sl_sl on z_sl.id = z_sl_sl.id_up	-- Реестр_законченных_случаев. ТЧ Сведения о случае
inner join ks_ddlcontrol.sl on sl.id = z_sl_sl.sl_spr -- ТЧ Сведения о случае. Реестр случаев
inner join ks_ddlcontrol.sl_naz on sl.id = sl_naz.id_up -- ТЧ Сведения о случае. Реестр случаев. ТЧ Назначения
inner join ks_ddlcontrol.rs015 on -- ТЧ Сведения о случае. Реестр случаев. Диспансерное наблюдение
		rs015.id = sl.dn_rs015 
	and rs015.kod = 1
inner join t_d as d0 on 
		d0.Код_МО = d.code_mo_f003 -- Код МО
	and d0.Год = d.year	-- Отчетный год
	and d0.Плановый_период_посещения = d.month	-- Отчетный месяц
where d.documentid in (3721, 3735) 
;

delete from t_inst where 
	Дата != (
		select max(Дата)		
		from t_inst as t
		where coalesce(t_inst.спр_СМО_Плательщик,0) = coalesce(t.спр_СМО_Плательщик,0)
	)
;

delete from t_inst where Пациент not in (select Пациент from t_d)
;

-- - если такая строка есть, то обновить данные в полях в ТЧ Запись. Карточка профилактического мероприятия. ТЧ История прохождения
update ks_ddlcontrol.d001_visit
set 
date_fact = t_inst.Дата_оказания_МП,--	Дата оказания МП
rezult = t_inst.Результат_обращения, --	Классификатор результатов обращения за медицинской помощью (Rezult)
mkb_fact = t_inst.Диагноз_основной, --	Основной диагноз по результатам диспансерного осмотра
mo_fact = t_inst.МО_по_результатам --	МО по результатам диспансерного осмотра
from t_d
inner join t_inst on t_d.Пациент = t_inst.Пациент and t_d.Плановый_период_посещения = t_inst.Отчетный_месяц
where ks_ddlcontrol.d001_visit.id = t_d.История_прохождения
;

-- - если в текущем документе есть строка и по ней заполнен атрибут «Дата оказания МП», которой нет в найденных документах «Сведения об оказанной медицинской помощи (C файл)», «Сведения об оказанной медицинской помощи (H файл)», 
-- то очистить атрибуты данной строки (кроме Плановый период посещения. Код, Место проведения диспансерного приема)
update ks_ddlcontrol.d001_visit
set 
date_fact = null,--	Дата оказания МП
rezult = null, --	Классификатор результатов обращения за медицинской помощью (Rezult)
mkb_fact = null, --	Основной диагноз по результатам диспансерного осмотра
mo_fact = null --	МО по результатам диспансерного осмотра
from t_d
where ks_ddlcontrol.d001_visit.id = t_d.История_прохождения
	and ks_ddlcontrol.d001_visit.date_fact is not null
	and not exists (
		select 
		*
		from t_inst
		where t_d.Пациент = t_inst.Пациент and t_d.Плановый_период_посещения = t_inst.Отчетный_месяц
	)
;



end
-- $$;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3395_fill_3107_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

declare
v_Unit uuid;

begin 
	
select dbo.sys_guid() 
into v_Unit;

create temporary table t_doc on commit drop as
select 
id,
status,
f002 as смо,
zl_list as Реестр_счетов
from ks_ddlcontrol.exp001
where id = v_doc_id and status=141 and coalesce(f002,0)=0;

create temporary table t_Реестр_случаев on commit drop as
Select sl_spr as Реестр_случаев, z_sl_spr as Реестр_законченных_случаев, pacient_spr as Сведения_о_пациенте 
from t_doc 
inner join ks_ddlcontrol.zl_list d on d.id=t_doc.Реестр_счетов
inner join ks_ddlcontrol.zl_list_zap z on d.id=z.id_up 
inner join ks_ddlcontrol.z_sl s_rs on  s_rs.id=z.z_sl_spr 
inner join ks_ddlcontrol.z_sl_sl s_rs_ss on s_rs.id=s_rs_ss.id_up
;

insert into ks_ddlcontrol.exp001_zsl (
unit,
guid,
id_up,
sl,
zsl,
pacient
)
--create temporary table t_temp on commit drop as
select 
v_Unit,
dbo.sys_guid(),
v_doc_id,
Реестр_случаев,
Реестр_законченных_случаев,
Сведения_о_пациенте
from t_Реестр_случаев rs
where  not exists (Select * from ks_ddlcontrol.exp001_zsl zs where id_up=v_doc_id and coalesce(zs.sl,0)=coalesce(rs.Реестр_случаев,0)  and coalesce(zs.zsl,0)=coalesce(rs.Реестр_законченных_случаев,0)  and coalesce(zs.pacient,0)=coalesce(rs.Сведения_о_пациенте,0))
;

create temporary table t_оплата on commit drop as
select sum(sumv) as sumv
from ks_ddlcontrol.exp001_zsl zsl
inner join ks_ddlcontrol.z_sl s_rs on  s_rs.id=zsl.zsl 
where zsl.id_up = v_doc_id
;

update  ks_ddlcontrol.exp001 target
	set topay = sumv
--create temporary table t_temp on commit drop as	
--select sumv
from ks_ddlcontrol.exp001 zsl
inner join t_оплата on 1=1
where zsl.id = v_doc_id and target.id = zsl.id	
;

end;

$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3398(v_nmode integer DEFAULT 0, v_table_name_result character varying DEFAULT ''::character varying, v_ckiguid character varying DEFAULT ''::character varying, "v_статус" text DEFAULT '___Все___'::text, "v_дата" text DEFAULT '2020-01-01 00:00:00'::text, "v_год" text DEFAULT '3'::text, "v_месяц" text DEFAULT '1'::text, "v_едизм" text DEFAULT '1'::text, "v_округление" text DEFAULT '0'::text)
 RETURNS TABLE(ord text, col_01 character varying, col_02 text, col_03 character varying, col_04 character varying, col_05 character varying, col_06 character varying, col_07 character varying, col_08 character varying, col_09 character varying, col_10 character varying, col_11 character varying, col_12 character varying, col_13 character varying, col_14 character varying, col_15 character varying, col_16 character varying, col_17 character varying, col_18 character varying, col_19 character varying, col_20 character varying, col_21 character varying, col_22 character varying, col_23 character varying, col_24 character varying, col_25 character varying, col_26 character varying, col_27 character varying, col_28 character varying, col_29 character varying, col_30 character varying, col_31 character varying, col_32 character varying, col_33 character varying, col_34 character varying, col_34_1 character varying, col_35 character varying, col_36 character varying, col_37 character varying, col_38 character varying, col_39 character varying, col_39_1 character varying)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

-- отчет "Сведения о результатах проведения диспансеризации взрослого населения"

declare

v_table_name_result character varying DEFAULT '';
v_ckiguid character varying DEFAULT '';
v_dt varchar(8);
v_k varchar(1);
v_temp_result varchar(254);
v_tmp_guid character varying default null;
v_sel text;
v_seldel text;
v_msg_text text;
v_except_detail text;
v_except_hint text;

t_реестр character varying default null;
t_списки_план character varying default null;
t_списки_факт character varying default null;
t_информирование character varying default null;


begin
	
	
v_tmp_guid := replace(cast(dbo.sys_guid() as varchar(36)),'-','_');

t_реестр := concat('Запрос_53937598_',v_tmp_guid);
t_списки_план := concat('Запрос_56911149_',v_tmp_guid);
t_списки_факт := concat('Запрос_74826507_',v_tmp_guid);
t_информирование := concat('Запрос_60071847_',v_tmp_guid);



v_seldel := '
drop table if exists ' || t_реестр || ';
drop table if exists ' || t_списки_план || ';
drop table if exists ' || t_списки_факт || ';
drop table if exists ' || t_информирование || ';';

begin 
	perform KS_DDLCONTROL.SP_DS_QUERY_6854( v_nmode := v_nmode, v_table_name_result := t_реестр, v_ckiguid := v_ckiguid, v_дата := v_дата, v_статус := v_статус);
	perform KS_DDLCONTROL.SP_DS_QUERY_6858( v_nmode := v_nmode, v_table_name_result := t_списки_план, v_ckiguid := v_ckiguid, v_дата := v_дата, v_год := v_год);
	perform KS_DDLCONTROL.SP_DS_QUERY_6863( v_nmode := v_nmode, v_table_name_result := t_списки_факт, v_ckiguid := v_ckiguid, v_дата := v_дата, v_год := v_год);
  	perform KS_DDLCONTROL.SP_DS_QUERY_6860( v_nmode := v_nmode, v_table_name_result := t_информирование, v_ckiguid := v_ckiguid, v_дата := v_дата, v_год := v_год , v_месяц := v_месяц);

exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	execute (v_seldel);
	RAISE EXCEPTION 'ошибка получения исходных данных: %', v_msg_text
      USING HINT = 'Проверьте используемые запросы.';

end; 

v_temp_result := concat('temp_result_',v_tmp_guid);
v_dt := left(replace(v_дата,'-',''),8);
v_k := case when cast(v_едизм as int) <=1000 then 2 else 1 end;

v_sel := '

create temporary table ' || v_temp_result || '(
ord text null,
col_01 varchar(4000) null,
col_02 text null,
col_03 varchar(4000) null,
col_04 varchar(4000) null,
col_05 varchar(4000) null,
col_06 varchar(4000) null,
col_07 varchar(4000) null,
col_08 varchar(4000) null,
col_09 varchar(4000) null,
col_10 varchar(4000) null,
col_11 varchar(4000) null,
col_12 varchar(4000) null,
col_13 varchar(4000) null,
col_14 varchar(4000) null,
col_15 varchar(4000) null,
col_16 varchar(4000) null,
col_17 varchar(4000) null,
col_18 varchar(4000) null,
col_19 varchar(4000) null,
col_20 varchar(4000) null,
col_21 varchar(4000) null,
col_22 varchar(4000) null,
col_23 varchar(4000) null,
col_24 varchar(4000) null,
col_25 varchar(4000) null,
col_26 varchar(4000) null,
col_27 varchar(4000) null,
col_28 varchar(4000) null,
col_29 varchar(4000) null,
col_30 varchar(4000) null,
col_31 varchar(4000) null,
col_32 varchar(4000) null,
col_33 varchar(4000) null,
col_34 varchar(4000) null,
col_34_1 varchar(4000) null,
col_35 varchar(4000) null,
col_36 varchar(4000) null,
col_37 varchar(4000) null,
col_38 varchar(4000) null,
col_39 varchar(4000) null,
col_39_1 varchar(4000) null
) on commit drop;

create temporary table t_temp_result_0(
ord text null,
col_01 varchar(4000) null,
col_02 text null,
col_03 varchar(4000) null default ''X'',
col_04 varchar(4000) null default ''X'',
col_05 int null default 0,
col_06 int null default 0,
col_07 int null default 0,
col_08 numeric(17,'||v_k||') null default 0.00,
col_09 int null default 0,
col_10 int null default 0,
col_11 numeric(17,'||v_k||') null default 0.00,
col_12 int null default 0,
col_13 int null default 0,
col_14 numeric(17,'||v_k||') null default 0.00,
col_15 int null default 0,
col_16 numeric(17,'||v_k||') null default 0.00,
col_17 numeric(17,'||v_k||') null default 0.00,
col_18 int null default 0,
col_19 int null default 0,
col_20 int null default 0,
col_21 numeric(17,'||v_k||') null default 0.00,
col_22 int null default 0,
col_23 int null default 0,
col_24 int null default 0,
col_25 numeric(17,'||v_k||') null default 0.00,
col_26 int null default 0,
col_27 numeric(17,'||v_k||') null default 0.00,
col_28 int null default 0,
col_29 int null default 0,
col_30 int null default 0,
col_31 int null default 0,
col_32 int null default 0,
col_33 int null default 0,
col_34 int null default 0,
col_34_1 int null default 0,
col_35 int null default 0,
col_36 int null default 0,
col_37 int null default 0,
col_38 int null default 0,
col_39 int null default 0,
col_39_1 int null default 0
) on commit drop;

delete from ' || t_реестр || '  
where  not (
	отчетный_год in (select year from ks_ddlcontrol.spr_year where id = 0'|| v_год ||')
	and 
	отчетный_месяц in (select kod from ks_ddlcontrol.tsp018 where id = 0'|| v_месяц ||')
);

delete from ' || t_реестр || ' where
дата <> (
        select  max(t.дата)
        from   ' || t_реестр || ' as t
        where 
			' || t_реестр || '.код_мо = t.код_мо 
			and ' || t_реестр || '.тип_диспансеризации = t.тип_диспансеризации
            );

ALTER TABLE ' || t_реестр || '
ADD возраст int NULL;

update ' || t_реестр || ' set
возраст = отчетный_год-cast(to_char(пациент_дата_рождения, ''YYYY'') as int),
сумма_выставленная = case when 0'|| v_округление ||' = 0 then  round(сумма_выставленная/cast(' || coalesce(v_едизм,'1') || ' as int), '||v_k||') else сумма_выставленная/cast(' || coalesce(v_едизм,'1') || ' as int) end,
сумма_принятая = case when 0'|| v_округление ||' = 0 then  round(сумма_принятая/cast(' || coalesce(v_едизм,'1') || ' as int), '||v_k||') else сумма_принятая/cast(' || coalesce(v_едизм,'1') || ' as int) end
;


delete from ' || t_списки_план || ' where  not (
 	cast(to_char(дата, ''YYYYMMDD'') as int) <= 0' || left(replace(v_дата,'-',''),8) || '
);

delete from ' || t_списки_план || ' where
дата <> (
        select  max(t.дата)
        from   ' || t_списки_план || ' as t
        where 
			' || t_списки_план || '.код_мо = t.код_мо 
            );


ALTER TABLE ' || t_списки_план || '
ADD возраст int NULL;

update ' || t_списки_план || ' set
возраст = год_год-cast(to_char(пациент_дата_рождения, ''YYYY'') as int);

delete from ' || t_списки_факт || '  
where  not (
	coalesce(cast(to_char(дата_оказания_мп, ''MM'') as int),0) in (select kod from ks_ddlcontrol.tsp018 where id = 0'|| v_месяц ||')
);

delete from ' || t_списки_факт || ' where
дата <> (
        select  max(t.дата)
        from   ' || t_списки_факт || ' as t
        where 
			' || t_списки_факт || '.код_мо = t.код_мо 
            );

ALTER TABLE ' || t_списки_факт || '
ADD возраст int NULL;

update ' || t_списки_факт || ' set
возраст = год_год-cast(to_char(пациент_дата_рождения, ''YYYY'') as int);

delete from ' || t_информирование || ' where  not (
 	cast(to_char(дата, ''YYYYMMDD'') as int) <= 0' || left(replace(v_дата,'-',''),8) || '
);

delete from ' || t_информирование || ' where
дата <> (
        select  max(t.дата)
        from   ' || t_информирование || ' as t
        where 
			' || t_информирование || '.код_мо = t.код_мо 
            );



insert into t_temp_result_0(col_01,col_02,col_05,col_07)
select

case 
	when 18<=возраст  and возраст <=39 then ''подлежащие диспансеризации 1 раз в 3 года в возрасте от 18 до 39 лет''
	when 40<=возраст  and возраст <=64 then ''40-64 лет''
	when возраст =65 then ''65 лет''
	when возраст>64 then ''старше 65 лет''
end,
case 
	when пациент_пол=1 and 18<=возраст  and возраст <=39 then ''3''
	when пациент_пол=1 and 40<=возраст  and возраст <=64 then ''5''
	when пациент_пол=1 and возраст =65 then ''6''
	when пациент_пол=1 and возраст>64 then ''7''
	when пациент_пол=2 and 18<=возраст  and возраст <=39 then ''9''
	when пациент_пол=2 and 40<=возраст  and возраст <=64 then ''11''
	when пациент_пол=2 and возраст =65 then ''12''
	when пациент_пол=2 and возраст>64 then ''13''
end,
1,
case when месяц_id=0'|| v_месяц ||' then 1 else 0 end
from ' || t_списки_план || ';




insert into t_temp_result_0(col_01,col_02,col_28,col_29,col_30,col_31,col_32,col_33,col_34,col_34_1,col_35,col_36,col_37,col_38,col_39,col_39_1)
select
case 
	when 18<=возраст  and возраст <=39 then ''подлежащие диспансеризации 1 раз в 3 года в возрасте от 18 до 39 лет''
	when 40<=возраст  and возраст <=64 then ''40-64 лет''
	when возраст =65 then ''65 лет''
	when возраст>64 then ''старше 65 лет''
end,
case 
	when пациент_пол=1 and 18<=возраст  and возраст <=39 then ''3''
	when пациент_пол=1 and 40<=возраст  and возраст <=64 then ''5''
	when пациент_пол=1 and возраст =65 then ''6''
	when пациент_пол=1 and возраст>64 then ''7''
	when пациент_пол=2 and 18<=возраст  and возраст <=39 then ''9''
	when пациент_пол=2 and 40<=возраст  and возраст <=64 then ''11''
	when пациент_пол=2 and возраст =65 then ''12''
	when пациент_пол=2 and возраст>64 then ''13''
end,
case when результат_диспансеризации_id in (13,14) then 1 else 0 end,
case when результат_диспансеризации_id in (13,14) and признак_отказа=1 then 1 else 0 end,
case when результат_диспансеризации_id in (3) then 1 else 0 end,
case when результат_диспансеризации_id in (4) then 1 else 0 end,
case when результат_диспансеризации_id in (19) then 1 else 0 end,
case when результат_диспансеризации_id in (20) then 1 else 0 end,
case when направлен_на_консультацию=1 then 1 else 0 end,
case when направлен_на_консультацию=1 and признак_подозрения=1 then 1 else 0 end,
case when направлен_на_обследование=1 then 1 else 0 end,
case when направлен_в_дневной_стационар=1 then 1 else 0 end,
case when направлен_на_госпитализацию=1 then 1 else 0 end,
case when направлен_в_реабилитационное_отд=1 then 1 else 0 end,
case when установлено_диспансерное_набл=1 then 1 else 0 end,
case when установлено_диспансерное_набл=1 and злокачественное_новообразование=1 then 1 else 0 end
from ' || t_списки_факт || ';


insert into t_temp_result_0(col_01,col_02,col_08,col_09,col_11,col_12,col_14,col_17,col_18,col_21,col_22,col_25)
select
case 
	when 18<=возраст  and возраст <=39 then ''подлежащие диспансеризации 1 раз в 3 года в возрасте от 18 до 39 лет''
	when 40<=возраст  and возраст <=64 then ''40-64 лет''
	when возраст =65 then ''65 лет''
	when возраст>64 then ''старше 65 лет''
end,
case 
	when пациент_пол=1 and 18<=возраст  and возраст <=39 then ''3''
	when пациент_пол=1 and 40<=возраст  and возраст <=64 then ''5''
	when пациент_пол=1 and возраст =65 then ''6''
	when пациент_пол=1 and возраст>64 then ''7''
	when пациент_пол=2 and 18<=возраст  and возраст <=39 then ''9''
	when пациент_пол=2 and 40<=возраст  and возраст <=64 then ''11''
	when пациент_пол=2 and возраст =65 then ''12''
	when пациент_пол=2 and возраст>64 then ''13''
end,
сумма_выставленная,
case when тип_диспансеризации=''ДВ4'' and сумма_выставленная is not null then 1 else 0 end,
case when тип_диспансеризации=''ДВ4'' then сумма_выставленная else 0 end,
case when тип_диспансеризации=''ДВ2'' and сумма_выставленная is not null then 1 else 0 end,
case when тип_диспансеризации=''ДВ2'' then сумма_выставленная else 0 end,
сумма_принятая,
case when тип_диспансеризации=''ДВ4'' and сумма_принятая is not null then 1 else 0 end,
case when тип_диспансеризации=''ДВ4'' then сумма_принятая else 0 end,
case when тип_диспансеризации=''ДВ2'' and сумма_принятая is not null then 1 else 0 end,
case when тип_диспансеризации=''ДВ2'' then сумма_принятая else 0 end
from ' || t_реестр || ';

insert into t_temp_result_0(col_01,col_02,col_20)
select
case 
	when 18<=возраст  and возраст <=39 then ''подлежащие диспансеризации 1 раз в 3 года в возрасте от 18 до 39 лет''
	when 40<=возраст  and возраст <=64 then ''40-64 лет''
	when возраст =65 then ''65 лет''
	when возраст>64 then ''старше 65 лет''
end,
case 
	when r.пациент_пол=1 and 18<=возраст  and возраст <=39 then ''3''
	when r.пациент_пол=1 and 40<=возраст  and возраст <=64 then ''5''
	when r.пациент_пол=1 and возраст =65 then ''6''
	when r.пациент_пол=1 and возраст>64 then ''7''
	when r.пациент_пол=2 and 18<=возраст  and возраст <=39 then ''9''
	when r.пациент_пол=2 and 40<=возраст  and возраст <=64 then ''11''
	when r.пациент_пол=2 and возраст =65 then ''12''
	when r.пациент_пол=2 and возраст>64 then ''13''
end,
1
from ' || t_информирование || ' i
inner join (Select distinct пациент_id, пациент_пол, возраст from ' || t_реестр || ' where coalesce(сумма_принятая, 0)<>0 ) r on i.пациент_id=r.пациент_id
;


update t_temp_result_0 set 
ord = right(''00''||col_02,2),
col_06 = col_05,
col_10 = col_09,
col_13 = col_12,
col_19 = col_18,
col_23 = col_22,
col_24 = col_22
;



insert into t_temp_result_0 (ord, col_01, col_02)
Select * from (
Select  
''03'' сортировка,
''подлежащие диспансеризации 1 раз в 3 года в возрасте от 18 до 39 лет'' Наименование,
''3'' строка
union 
Select  
''05'' сортировка,
''40-64 лет'' Наименование,
''5'' строка
union 
Select  
''06'' сортировка,
''65 лет'' Наименование,
''6'' строка
union 
Select  
''07'' сортировка,
''старше 65 лет'' Наименование,
''7'' строка
union 
Select  
''09'' сортировка,
''подлежащие диспансеризации 1 раз в 3 года в возрасте от 18 до 39 лет'' Наименование,
''9'' строка
union 
Select  
''11'' сортировка,
''40-64 лет'' Наименование,
''11'' строка
union 
Select  
''12'' сортировка,
''65 лет'' Наименование,
''12'' строка
union 
Select  
''13'' сортировка,
''старше 65 лет'' Наименование,
''13'' строка
) d;

insert into t_temp_result_0
Select 
''04'',
''подлежащие диспансеризации ежегодно (сумма стр. =5+6+7)'',
''4'',
col_03,col_04,col_05,col_06,col_07,col_08,col_09,col_10,col_11,col_12,col_13,col_14,col_15,col_16,col_17,col_18,col_19,col_20,col_21,col_22,col_23,col_24,col_25,col_26,col_27,col_28,col_29,col_30,col_31,col_32,col_33,col_34,col_34_1,col_35,col_36,col_37,col_38,col_39,col_39_1
from t_temp_result_0 where col_02 in (''5'',''6'',''7'');

insert into t_temp_result_0
Select 
''02'',
''Мужчины: всего, в том числе (сумма строк = 3+4)'',
''2'',
col_03,col_04,col_05,col_06,col_07,col_08,col_09,col_10,col_11,col_12,col_13,col_14,col_15,col_16,col_17,col_18,col_19,col_20,col_21,col_22,col_23,col_24,col_25,col_26,col_27,col_28,col_29,col_30,col_31,col_32,col_33,col_34,col_34_1,col_35,col_36,col_37,col_38,col_39,col_39_1
from t_temp_result_0 where col_02 in (''3'',''4'');


insert into t_temp_result_0
Select 
''10'',
''подлежащие диспансеризации ежегодно (сумма строк =11+12+13)'',
''10'',
col_03,col_04,col_05,col_06,col_07,col_08,col_09,col_10,col_11,col_12,col_13,col_14,col_15,col_16,col_17,col_18,col_19,col_20,col_21,col_22,col_23,col_24,col_25,col_26,col_27,col_28,col_29,col_30,col_31,col_32,col_33,col_34,col_34_1,col_35,col_36,col_37,col_38,col_39,col_39_1
from t_temp_result_0 where col_02 in (''11'',''12'',''13'');


insert into t_temp_result_0
Select 
''08'',
''Женщины: всего, в том числе (сумма строк = 9+10)'',
''8'' ,
col_03,
col_04,col_05,col_06,col_07,col_08,col_09,col_10,col_11,col_12,col_13,col_14,col_15,col_16,col_17,col_18,col_19,col_20,col_21,col_22,col_23,col_24,col_25,col_26,col_27,col_28,col_29,col_30,col_31,col_32,col_33,col_34,col_34_1,col_35,col_36,col_37,col_38,col_39,col_39_1
from t_temp_result_0 where col_02 in (''9'',''10'');

insert into t_temp_result_0
Select 
''01'',
''ВСЕГО, в том числе:(сумма строк=2+8)'',
''1'' ,
col_03,col_04,col_05,col_06,col_07,col_08,col_09,col_10,col_11,col_12,col_13,col_14,col_15,col_16,col_17,col_18,col_19,col_20,col_21,col_22,col_23,col_24,col_25,col_26,col_27,col_28,col_29,col_30,col_31,col_32,col_33,col_34,col_34_1,col_35,col_36,col_37,col_38,col_39,col_39_1
from t_temp_result_0 where col_02 in (''2'',''8'');

insert into ' || v_temp_result || '
Select 
ord,
col_01,
col_02,
col_03,
col_04,
sum(col_05) as col_05,
sum(col_06) as col_06,
sum(col_07) as col_07,
case when 0'|| v_округление ||' = 1 then  round(sum(col_08), '||v_k||') else sum(col_08) end,
sum(col_09) as col_09,
sum(col_10) as col_10,
case when 0'|| v_округление ||' = 1 then  round(sum(col_11), '||v_k||') else sum(col_11) end,
sum(col_12) as col_12,
sum(col_13) as col_13,
case when 0'|| v_округление ||' = 1 then  round(sum(col_14), '||v_k||') else sum(col_14) end,
sum(col_15) as col_15,
sum(col_16) as col_16,
case when 0'|| v_округление ||' = 1 then  round(sum(col_17), '||v_k||') else sum(col_17) end,
sum(col_18) as col_18,
sum(col_19) as col_19,
sum(col_20) as col_20,
case when 0'|| v_округление ||' = 1 then  round(sum(col_21), '||v_k||') else sum(col_21) end,
sum(col_22) as col_22,
sum(col_23) as col_23,
sum(col_24) as col_24,
case when 0'|| v_округление ||' = 1 then  round(sum(col_25), '||v_k||') else sum(col_25) end,
sum(col_26) as col_26,
sum(col_27) as col_27,
sum(col_28) as col_28,
sum(col_29) as col_29,
sum(col_30) as col_30,
sum(col_31) as col_31,
sum(col_32) as col_32,
sum(col_33) as col_33,
sum(col_34) as col_34,
sum(col_34_1) as col_34_1,
sum(col_35) as col_35,
sum(col_36) as col_36,
sum(col_37) as col_37,
sum(col_38) as col_38,
sum(col_39) as col_39,
sum(col_39_1) as col_39_1
from t_temp_result_0 
group by
ord,
col_01,
col_02,
col_03,
col_04
;

update ' || v_temp_result || '
set col_03 = (Select count(DISTINCT код_мо) from ' || t_реестр || ')
where col_02=''1''


';
           
begin 
	execute (v_sel);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	execute (v_seldel);
	RAISE EXCEPTION 'ошибка формирования результата отчета: %', v_msg_text
      USING HINT = v_sel;

end; 

execute (v_seldel);


-- Финальный процесс
if coalesce(v_table_name_result,'') <> '' then

      v_sel := concat('drop table if exists ', v_table_name_result,';create temporary table ', v_table_name_result , ' on commit drop as select  * from ' , v_temp_result , '    ');
      execute v_sel;

else

      v_sel := concat('select  * from ', v_temp_result, ' order by ord   ');
      return query execute v_sel;

end if;
		
end;

$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3473_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- МИД 3473 Планирование АПП/ 4087 АПП (расчет базовой ставки)
-- 2.	Действия при сохранении документа

declare
--v_doc_id integer;
  v_doc_new integer;

begin 

--v_doc_id := 1;

-- select * from KS_DDLControl.ds_3473_23133
--delete from KS_DDLControl.zl_list where left(filename, 1)='D'  and st_owner=92 


create temporary table t_d on commit drop as -- текущий документ
select 
id,
years,
date,
os_fap as ОС_ФАП,
count_zl as Число_ЗЛ
from KS_DDLControl.ds_3473_23133 as d -- МИД 3473 Планирование АПП/ 4087 АПП (расчет базовой ставки)
where d.id = v_doc_id
;
create temporary table t_inst on commit drop as -- исходные данные
select 
d.id,
d.year,
d.date
from t_d  
inner join ks_ddlcontrol.tp001 as d  -- МИД 3292 «Нормативы объема медицинской помощи»/ документ 3906 «Нормативы объема медицинской помощи»
	on d.documentid = 3906 
	and d.date <= t_d.date 
	and d.year = t_d.years 
;
delete from t_inst where 
	date <> (
		select max(date)
		from t_inst as t
	)
;
if not exists (select * from t_inst) then
	RAISE notice '"Нормативы объема медицинской помощи" не найден';
end if
;

create temporary table t_inst_t on commit drop as -- Нормативы объема медицинской помощи . ТЧ Таблица
select 
t.cl_138964 as Виды_условия_формы,
t.norm as Норматив_ТПОМС
from t_inst as d
inner join ks_ddlcontrol.tp001_contents as t on t.id_up = d.id
;

update ks_ddlcontrol.ds_3473_23143	-- текущий документ . ТЧ Показатели
set vol_standart_tp	= t_inst_t.Норматив_ТПОМС -- Норматив объема по ТП
from t_inst_t
where t_inst_t.Виды_условия_формы = ks_ddlcontrol.ds_3473_23143.kinds_types and ks_ddlcontrol.ds_3473_23143.id_up = v_doc_id
;
update ks_ddlcontrol.ds_3473_23143	-- текущий документ . ТЧ Показатели
set vol_standart_tp	= null
where ks_ddlcontrol.ds_3473_23143.kinds_types not in (select Виды_условия_формы from t_inst_t) and ks_ddlcontrol.ds_3473_23143.id_up = v_doc_id
;

create temporary table t_inst4 on commit drop as -- исходные данные
select 
d.id,
d.year,
d.date
from t_d  
inner join ks_ddlcontrol.tp004 as d  -- МИД 3297 «Нормативы финансовых затрат на единицу объема медицинской помощи для ТП»/ документ 3911 «Нормативы финансовых затрат на единицу объема медицинской помощи для ТП» 
	on d.documentid = 3911 
	and d.date <= t_d.date 
	and d.year = t_d.years 
;
delete from t_inst4 where 
	date <> (
		select max(date)
		from t_inst4 as t
	)
;
if not exists (select * from t_inst) then
	RAISE notice '"Нормативы финансовых затрат на единицу объема медицинской помощи для ТП" не найден';
end if
;
create temporary table t_inst_t4 on commit drop as -- Нормативы финансовых затрат на единицу объема медицинской помощи для ТП . ТЧ Таблица
select 
t.tpp001 as Виды_условия_формы,
t.norm as Норматив_ТПОМС
from t_inst4 as d
inner join ks_ddlcontrol.tp004_contents as t on t.id_up = d.id
;

update ks_ddlcontrol.ds_3473_23143	-- текущий документ . ТЧ Показатели
set fin_standart_tp	= t_inst_t4.Норматив_ТПОМС -- Норматив затрат по ТП
from t_inst_t4
where t_inst_t4.Виды_условия_формы = ks_ddlcontrol.ds_3473_23143.kinds_types and ks_ddlcontrol.ds_3473_23143.id_up = v_doc_id
;
update ks_ddlcontrol.ds_3473_23143	-- текущий документ . ТЧ Показатели
set fin_standart_tp	= null
where ks_ddlcontrol.ds_3473_23143.kinds_types not in (select Виды_условия_формы from t_inst_t4) and ks_ddlcontrol.ds_3473_23143.id_up = v_doc_id
;

-- 3)	Рассчитать значения ФО абм, НфзПроф, ПНА по формулам 
create temporary table v_ФО_амб on commit drop as
select	cl_140828 as Виды
from ks_ddlcontrol.ds_3473_23174 as t	-- текущий документ . ТЧ Настройки для расчета ФО амб
where id_up = v_doc_id
;
create temporary table v_НфзПроф on commit drop as
select	cl_140829 as Виды
from ks_ddlcontrol.ds_3473_23176 as t	-- текущий документ . ТЧ Настройки для расчета НфзПроф
where id_up = v_doc_id
;
create temporary table v_ПНА on commit drop as
select	cl_140832 as Виды
from ks_ddlcontrol.ds_3473_23180 as t	-- текущий документ . ТЧ Настройки для расчета ПНА
where id_up = v_doc_id
;
/*
 	kinds_types as Виды_условия_формы,
	fin_standart_rf as Норматив_затрат_РФ,
	fin_standart_tp as Норматив_затрат_по_ТП,
	pay_volume_mp_mtr as Средства_на_оплату_МТР,
	pay_volume_mp as Средства_на_оплату_МП,
	pay_volume_mp_sum as Средства_на_оплату_всего,
	sum_volume_mp as Общий_объем_МП,
	volume_mp_sub as Объем_МП_в_субъекте,
	volume_mp_mtr as Объем_МТР,
	vol_standart_tp as Норматив_объема_по_ТП,
	fin_standart_sub as Норматив_затрат_субъект,
 */	
update KS_DDLControl.ds_3473_23133 
set fo_amb = v.new_z --	ФО амб
from (
	select
	( sum( fin_standart_rf * fin_standart_tp ) * d.count_zl - sum(pay_volume_mp_mtr) ) / d.count_zl as new_z
	from KS_DDLControl.ds_3473_23133 as d
	inner join ks_ddlcontrol.ds_3473_23143 as t	-- текущий документ . ТЧ Показатели
		on t.id_up = d.id and t.kinds_types in (select Виды from v_ФО_амб)
	where d.id = v_doc_id
	group by d.count_zl
) as v
where KS_DDLControl.ds_3473_23133.id = v_doc_id
;
update KS_DDLControl.ds_3473_23133 
set stand_fz_prof = v.new_z --	НфзПроф
from (
	select
	sum( fin_standart_rf * fin_standart_tp ) / sum( fin_standart_tp ) as new_z
	from KS_DDLControl.ds_3473_23133 as d
	inner join ks_ddlcontrol.ds_3473_23143 as t	-- текущий документ . ТЧ Показатели
		on t.id_up = d.id and t.kinds_types in (select Виды from v_НфзПроф)
	where d.id = v_doc_id
) as v
where KS_DDLControl.ds_3473_23133.id = v_doc_id
;
update KS_DDLControl.ds_3473_23133 
set pna = v.new_z --	ПНА
from (
	select
	( fo_amb * d.count_zl - d.os_fap - sum(pay_volume_mp) ) / d.count_zl as new_z
	from KS_DDLControl.ds_3473_23133 as d
	inner join ks_ddlcontrol.ds_3473_23143 as t	-- текущий документ . ТЧ Показатели
		on t.id_up = d.id and t.kinds_types in (select Виды from v_ПНА)
	where d.id = v_doc_id
	group by d.fo_amb, d.count_zl, d.os_fap
) as v
where KS_DDLControl.ds_3473_23133.id = v_doc_id
;

end 
--$$;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3477_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- МИД 3477 Планирование_стационар/ 4091 Cтационар 
-- 2.	Действия при сохранении документа

declare
--v_doc_id integer;
  v_doc_new integer;

begin 

--v_doc_id := 4;

-- select * from KS_DDLControl.ds_3473_23133
--delete from KS_DDLControl.zl_list where left(filename, 1)='D'  and st_owner=92 


create temporary table t_d on commit drop as -- текущий документ
select 
id,
years,
date
from KS_DDLControl.ds_3477_23208 as d -- МИД 3477 Планирование_стационар/ 4091 Cтационар 
where d.id = v_doc_id
;
create temporary table t_inst on commit drop as -- исходные данные
select 
d.id,
d.year,
d.date
from t_d  
inner join ks_ddlcontrol.tp001 as d  -- МИД 3292 «Нормативы объема медицинской помощи»/ документ 3906 «Нормативы объема медицинской помощи»
	on d.documentid = 3906 
	and d.date <= t_d.date 
	and d.year = t_d.years 
;
delete from t_inst where 
	date <> (
		select max(date)
		from t_inst as t
	)
;
if not exists (select * from t_inst) then
	RAISE notice '"Нормативы объема медицинской помощи" не найден';
end if
;

create temporary table t_inst_t on commit drop as -- Нормативы объема медицинской помощи . ТЧ Таблица
select 
t.cl_138964 as Виды_условия_формы,
t.norm as Норматив_ТПОМС
from t_inst as d
inner join ks_ddlcontrol.tp001_contents as t on t.id_up = d.id
;

update ks_ddlcontrol.ds_3477_23217	-- текущий документ . ТЧ Показатели
set vol_standart_tp	= t_inst_t.Норматив_ТПОМС -- Норматив объема по ТП
from t_inst_t
where t_inst_t.Виды_условия_формы = ks_ddlcontrol.ds_3477_23217.kinds_types and ks_ddlcontrol.ds_3477_23217.id_up = v_doc_id
;

update ks_ddlcontrol.ds_3477_23217	-- текущий документ . ТЧ Показатели
set vol_standart_tp	= null
where ks_ddlcontrol.ds_3477_23217.kinds_types not in (select Виды_условия_формы from t_inst_t) and ks_ddlcontrol.ds_3477_23217.id_up = v_doc_id
;

create temporary table t_inst4 on commit drop as -- исходные данные
select 
d.id,
d.year,
d.date
from t_d  
inner join ks_ddlcontrol.tp004 as d  -- МИД 3297 «Нормативы финансовых затрат на единицу объема медицинской помощи для ТП»/ документ 3911 «Нормативы финансовых затрат на единицу объема медицинской помощи для ТП» 
	on d.documentid = 3911 
	and d.date <= t_d.date 
	and d.year = t_d.years 
;
delete from t_inst4 where 
	date <> (
		select max(date)
		from t_inst4 as t
	)
;
if not exists (select * from t_inst) then
	RAISE notice '"Нормативы финансовых затрат на единицу объема медицинской помощи для ТП" не найден';
end if
;
create temporary table t_inst_t4 on commit drop as -- Нормативы финансовых затрат на единицу объема медицинской помощи для ТП . ТЧ Таблица
select 
t.tpp001 as Виды_условия_формы,
t.norm as Норматив_ТПОМС
from t_inst4 as d
inner join ks_ddlcontrol.tp004_contents as t on t.id_up = d.id
;

update ks_ddlcontrol.ds_3477_23217	-- текущий документ . ТЧ Показатели
set fin_standart_tp	= t_inst_t4.Норматив_ТПОМС -- Норматив объема по ТП
from t_inst_t4
where t_inst_t4.Виды_условия_формы = ks_ddlcontrol.ds_3477_23217.kinds_types and ks_ddlcontrol.ds_3477_23217.id_up = v_doc_id
;
update ks_ddlcontrol.ds_3477_23217	-- текущий документ . ТЧ Показатели
set fin_standart_tp	= null
where ks_ddlcontrol.ds_3477_23217.kinds_types not in (select Виды_условия_формы from t_inst_t4) and ks_ddlcontrol.ds_3477_23217.id_up = v_doc_id
;


create temporary table t_inst10 on commit drop as -- исходные данные
select 
d.id,
d.year,
d.date
from t_d  
inner join ks_ddlcontrol.tp010 as d  -- МИД 3352 «Утвержденная стоимость ТП»/ документ 3966 «Утвержденная стоимость ТП» 
	on d.documentid = 3966 
	and d.date <= t_d.date 
	and d.year = t_d.years 
;
delete from t_inst10 where 
	date <> (
		select max(date)
		from t_inst10 as t
	)
;
if not exists (select * from t_inst) then
	RAISE notice '"Утвержденная стоимость ТП" не найден';
end if
;
create temporary table t_inst_t10 on commit drop as -- Утвержденная стоимость ТП . ТЧ Таблица офг
select 
tpp008 as Показатели_стоимости_ТП,
stooms as Стоимость_ОМС
from t_inst10 as d
inner join ks_ddlcontrol.tab as t on t.id_up = d.id
;

update ks_ddlcontrol.ds_3477_23217	-- текущий документ . ТЧ Показатели
set pay_volume_tp = t_inst_t10.Стоимость_ОМС -- Утвеждено ТП
from t_inst_t10
inner join KS_DDLControl.index_price as ip -- спр. Показатели стоимости ТП
	on ip.id = t_inst_t10.Показатели_стоимости_ТП 
where ip.tpp001 = ks_ddlcontrol.ds_3477_23217.kinds_types and ks_ddlcontrol.ds_3477_23217.id_up = v_doc_id
;

update ks_ddlcontrol.ds_3477_23217	-- текущий документ . ТЧ Показатели
set pay_volume_tp	= null
where ks_ddlcontrol.ds_3477_23217.kinds_types not in (
		select ip.tpp001
		from t_inst_t10
		inner join KS_DDLControl.index_price as ip -- спр. Показатели стоимости ТП
			on ip.id = t_inst_t10.Показатели_стоимости_ТП 
	) and ks_ddlcontrol.ds_3477_23217.id_up = v_doc_id
;


end 
--$$;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3478_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- МИД 3478 Планирование СМП/ 4095 СМП
-- 2.	Действия при сохранении документа

declare
--v_doc_id integer;
  v_doc_new integer;

begin 

--v_doc_id := 1;

-- select * from KS_DDLControl.ds_3478_23254
--delete from KS_DDLControl.zl_list where left(filename, 1)='D'  and st_owner=92 


create temporary table t_d on commit drop as -- текущий документ
select 
id,
years,
date
from KS_DDLControl.ds_3478_23254 as d -- - МИД 3478 Планирование СМП/ 4095 СМП
where d.id = v_doc_id
;
create temporary table t_inst on commit drop as -- исходные данные
select 
d.id,
d.year,
d.date
from t_d  
inner join ks_ddlcontrol.tp001 as d  -- МИД 3292 «Нормативы объема медицинской помощи»/ документ 3906 «Нормативы объема медицинской помощи»
	on d.documentid = 3906 
	and d.date <= t_d.date 
	and d.year = t_d.years 
;
delete from t_inst where 
	date <> (
		select max(date)
		from t_inst as t
	)
;
if not exists (select * from t_inst) then
	RAISE notice '"Нормативы объема медицинской помощи" не найден';
end if
;

create temporary table t_inst_t on commit drop as -- Нормативы объема медицинской помощи . ТЧ Таблица
select 
t.cl_138964 as Виды_условия_формы,
t.norm as Норматив_ТПОМС
from t_inst as d
inner join ks_ddlcontrol.tp001_contents as t on t.id_up = d.id
;
update ks_ddlcontrol.ds_3478_23263	-- текущий документ . ТЧ Показатели
set vol_standart_tp	= t_inst_t.Норматив_ТПОМС -- Норматив объема по ТП
from t_inst_t
where t_inst_t.Виды_условия_формы = ks_ddlcontrol.ds_3478_23263.kinds_types and ks_ddlcontrol.ds_3478_23263.id_up = v_doc_id
;
update ks_ddlcontrol.ds_3478_23263	-- текущий документ . ТЧ Показатели
set vol_standart_tp	= null
where ks_ddlcontrol.ds_3478_23263.kinds_types not in (select Виды_условия_формы from t_inst_t) and ks_ddlcontrol.ds_3478_23263.id_up = v_doc_id
;

create temporary table t_inst4 on commit drop as -- исходные данные
select 
d.id,
d.year,
d.date
from t_d  
inner join ks_ddlcontrol.tp004 as d  -- МИД 3297 «Нормативы финансовых затрат на единицу объема медицинской помощи для ТП»/ документ 3911 «Нормативы финансовых затрат на единицу объема медицинской помощи для ТП» 
	on d.documentid = 3911 
	and d.date <= t_d.date 
	and d.year = t_d.years 
;
delete from t_inst4 where 
	date <> (
		select max(date)
		from t_inst4 as t
	)
;
if not exists (select * from t_inst) then
	RAISE notice '"Нормативы финансовых затрат на единицу объема медицинской помощи для ТП" не найден';
end if
;
create temporary table t_inst_t4 on commit drop as -- Нормативы финансовых затрат на единицу объема медицинской помощи для ТП . ТЧ Таблица
select 
t.tpp001 as Виды_условия_формы,
t.norm as Норматив_ТПОМС
from t_inst4 as d
inner join ks_ddlcontrol.tp004_contents as t on t.id_up = d.id
;

update ks_ddlcontrol.ds_3478_23263	-- текущий документ . ТЧ Показатели
set fin_standart_tp	= t_inst_t4.Норматив_ТПОМС -- Норматив затрат по ТП
from t_inst_t4
where t_inst_t4.Виды_условия_формы = ks_ddlcontrol.ds_3478_23263.kinds_types and ks_ddlcontrol.ds_3478_23263.id_up = v_doc_id
;
update ks_ddlcontrol.ds_3478_23263	-- текущий документ . ТЧ Показатели
set fin_standart_tp	= null
where ks_ddlcontrol.ds_3478_23263.kinds_types not in (select Виды_условия_формы from t_inst_t4) and ks_ddlcontrol.ds_3478_23263.id_up = v_doc_id
;

end 
--$$;
 $function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3489(v_nmode integer DEFAULT 0, v_table_name_result character varying DEFAULT ''::character varying, v_ckiguid character varying DEFAULT ''::character varying, "v_Дата" text DEFAULT NULL::text, v_doc_id text DEFAULT NULL::text)
 RETURNS TABLE(ord character varying, "услуга__id" integer, "Услуга" text, "Тариф" numeric, "ст18m" numeric, "ст18w" numeric, "ст19m" numeric, "ст19w" numeric, "ст20m" numeric, "ст20w" numeric, "ст21m" numeric, "ст21w" numeric, "ст22m" numeric, "ст22w" numeric, "ст23m" numeric, "ст23w" numeric, "ст24m" numeric, "ст24w" numeric, "ст25m" numeric, "ст25w" numeric, "ст26m" numeric, "ст26w" numeric, "ст27m" numeric, "ст27w" numeric, "ст28m" numeric, "ст28w" numeric, "ст29m" numeric, "ст29w" numeric, "ст30m" numeric, "ст30w" numeric, "ст31m" numeric, "ст31w" numeric, "ст32m" numeric, "ст32w" numeric, "ст33m" numeric, "ст33w" numeric, "ст34m" numeric, "ст34w" numeric, "ст35m" numeric, "ст35w" numeric, "ст36m" numeric, "ст36w" numeric, "ст37m" numeric, "ст37w" numeric, "ст38m" numeric, "ст38w" numeric, "ст39m" numeric, "ст39w" numeric, "ст40m" numeric, "ст40w" numeric, "ст41m" numeric, "ст41w" numeric, "ст42m" numeric, "ст42w" numeric, "ст43m" numeric, "ст43w" numeric, "ст44m" numeric, "ст44w" numeric, "ст45m" numeric, "ст45w" numeric, "ст46m" numeric, "ст46w" numeric, "ст47m" numeric, "ст47w" numeric, "ст48m" numeric, "ст48w" numeric, "ст49m" numeric, "ст49w" numeric, "ст50m" numeric, "ст50w" numeric, "ст51m" numeric, "ст51w" numeric, "ст52m" numeric, "ст52w" numeric, "ст53m" numeric, "ст53w" numeric, "ст54m" numeric, "ст54w" numeric, "ст55m" numeric, "ст55w" numeric, "ст56m" numeric, "ст56w" numeric, "ст57m" numeric, "ст57w" numeric, "ст58m" numeric, "ст58w" numeric, "ст59m" numeric, "ст59w" numeric, "ст60m" numeric, "ст60w" numeric, "ст61m" numeric, "ст61w" numeric, "ст62m" numeric, "ст62w" numeric, "ст63m" numeric, "ст63w" numeric, "ст64m" numeric, "ст64w" numeric, "ст65m" numeric, "ст65w" numeric, "ст66m" numeric, "ст66w" numeric, "ст67m" numeric, "ст67w" numeric, "ст68m" numeric, "ст68w" numeric, "ст69m" numeric, "ст69w" numeric, "ст70m" numeric, "ст70w" numeric, "ст71m" numeric, "ст71w" numeric, "ст72m" numeric, "ст72w" numeric, "ст73m" numeric, "ст73w" numeric, "ст74m" numeric, "ст74w" numeric, "ст75m" numeric, "ст75w" numeric, "ст76m" numeric, "ст76w" numeric, "ст77m" numeric, "ст77w" numeric, "ст78m" numeric, "ст78w" numeric, "ст79m" numeric, "ст79w" numeric, "ст80m" numeric, "ст80w" numeric, "ст81m" numeric, "ст81w" numeric, "ст82m" numeric, "ст82w" numeric, "ст83m" numeric, "ст83w" numeric, "ст84m" numeric, "ст84w" numeric, "ст85m" numeric, "ст85w" numeric, "ст86m" numeric, "ст86w" numeric, "ст87m" numeric, "ст87w" numeric, "ст88m" numeric, "ст88w" numeric, "ст89m" numeric, "ст89w" numeric, "ст90m" numeric, "ст90w" numeric, "ст91m" numeric, "ст91w" numeric, "ст92m" numeric, "ст92w" numeric, "ст93m" numeric, "ст93w" numeric, "ст94m" numeric, "ст94w" numeric, "ст95m" numeric, "ст95w" numeric, "ст96m" numeric, "ст96w" numeric, "ст97m" numeric, "ст97w" numeric, "ст98m" numeric, "ст98w" numeric, "ст99m" numeric, "ст99w" numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

-- Отчет "ПФ 2 этап диспансеризации (Подотчет)"

--do $$

declare
/*
v_nmode integer DEFAULT 0;
 v_ckiguid character varying DEFAULT ''::character varying;
v_Дата text default '20201231' ;
v_doc_id text default '1' ;
*/
    v_sel text;
	v_seldel text;
   	v_sel1 text;
    v_temp_result varchar(254);
    v_table_Услуги character varying default null;
    v_table_ВозрастМ character varying default null;
	v_table_ВозрастЖ character varying default null;
	v_tmp_guid character varying default null;
	v_msg_text text;
	v_except_detail text;
	v_except_hint text;

	v_i integer;
	v_part_tbl text;


begin

if v_nmode=-1 then return;
end if;

v_tmp_guid := replace(cast(dbo.sys_guid() as varchar(36)),'-','_');

v_table_Услуги := concat('Запрос_63093249_',v_tmp_guid);
v_table_ВозрастМ := concat('Запрос_63442787_',v_tmp_guid);
v_table_ВозрастЖ := concat('Запрос_63574416_',v_tmp_guid);

v_seldel := '
drop table if exists ' || v_table_Услуги || ';
drop table if exists ' || v_table_ВозрастМ || ';
drop table if exists ' || v_table_ВозрастЖ || ';';

begin 
	perform ks_ddlcontrol.sp_ds_query_7221( v_nmode := v_nmode, v_table_name_result := v_table_Услуги, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_doc_id := v_doc_id);
	perform ks_ddlcontrol.sp_ds_query_7222( v_nmode := v_nmode, v_table_name_result := v_table_ВозрастМ, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_doc_id := v_doc_id);
	perform ks_ddlcontrol.sp_ds_query_7223( v_nmode := v_nmode, v_table_name_result := v_table_ВозрастЖ, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_doc_id := v_doc_id);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	RAISE EXCEPTION 'ошибка получения исходных данных: %', v_msg_text
      USING HINT = 'Проверьте используемые запросы.';

end; 

v_i := 18;
v_part_tbl := '';

while (v_i <= 99) loop
	v_part_tbl := v_part_tbl || ', ст'||right('00'||(v_i::text),2)||'m numeric(22,2) null';	
	v_part_tbl := v_part_tbl || ', ст'||right('00'||(v_i::text),2)||'w numeric(22,2) null';	
	v_i := v_i + 1;
end loop;

v_temp_result := concat('temp_result_',v_tmp_guid);

v_sel := '

drop table if exists ' || v_temp_result || ';

create temporary table ' || v_temp_result || '
(
	ord varchar(10)
 	, услуга__id integer
 	, Услуга text
 	, Тариф numeric(22,2) null
	' || v_part_tbl || '
) on commit drop;

insert into ' || v_temp_result || ' (ord, услуга__id, Услуга)
values(''00'', 0, ''ТАРИФ второго этапа диспансеризации, рублей'') ;

insert into ' || v_temp_result || ' (ord, услуга__id, Услуга, Тариф)
select 
right(''00''||cast(dense_rank() over(order by услуга__id) as varchar), 2) as ord,
услуга__id, услуга_наименование, услуга_тариф 
from ' || v_table_Услуги || ';

create temporary table t_ВозрастМ on commit drop as 
select услуга__id as id, возраст, стоимость from ' || v_table_ВозрастМ || ';

create temporary table t_ВозрастЖ on commit drop as 
select услуга__id as id, возраст, стоимость from ' || v_table_ВозрастЖ || ';

create temporary table t_tmp on commit drop as 
select * from ' || v_temp_result || ';

';

begin 
	execute (v_sel);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	execute (v_seldel);
	RAISE EXCEPTION 'ошибка формирования результата отчета: %', coalesce(v_msg_text,'')
      USING HINT = v_sel;

end; 

with s1 as (
	select 
		 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'m = '||cast(стоимость as varchar)||' where услуга__id = '||cast(id as varchar)||';'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'m = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'m,0)+'||cast(стоимость as varchar)||' where услуга__id = 0;' as sel
	from t_ВозрастМ
	union all
	select 
		'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'w = '||cast(стоимость as varchar)||' where услуга__id = '||cast(id as varchar)||';'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'w = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'w,0)+'||cast(стоимость as varchar)||' where услуга__id = 0;' as sel
	from t_ВозрастЖ
)
select 
string_agg(sel, chr(10))
into v_sel
from s1;

execute (v_sel);

execute (v_seldel);

-- Финальный процесс
if coalesce(v_table_name_result,'') <> '' then

      v_sel := concat('drop table if exists ', v_table_name_result,';create temporary table ', v_table_name_result , ' on commit drop as select  * from t_tmp order by ord ');
      execute v_sel;

else

      v_sel := concat('select  * from t_tmp order by ord  ');
      return query execute v_sel;

end if;

end;
--$$
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3490(v_nmode integer DEFAULT 0, v_table_name_result character varying DEFAULT ''::character varying, v_ckiguid character varying DEFAULT ''::character varying, "v_Дата" text DEFAULT NULL::text, v_doc_id text DEFAULT NULL::text)
 RETURNS TABLE(ord character varying, "Вид" text, "услуга__id" integer, "Услуга" text, "Тариф" numeric, "РайонКоэф" numeric, "ст18m" numeric, "ст18w" numeric, "ст19m" numeric, "ст19w" numeric, "ст20m" numeric, "ст20w" numeric, "ст21m" numeric, "ст21w" numeric, "ст22m" numeric, "ст22w" numeric, "ст23m" numeric, "ст23w" numeric, "ст24m" numeric, "ст24w" numeric, "ст25m" numeric, "ст25w" numeric, "ст26m" numeric, "ст26w" numeric, "ст27m" numeric, "ст27w" numeric, "ст28m" numeric, "ст28w" numeric, "ст29m" numeric, "ст29w" numeric, "ст30m" numeric, "ст30w" numeric, "ст31m" numeric, "ст31w" numeric, "ст32m" numeric, "ст32w" numeric, "ст33m" numeric, "ст33w" numeric, "ст34m" numeric, "ст34w" numeric, "ст35m" numeric, "ст35w" numeric, "ст36m" numeric, "ст36w" numeric, "ст37m" numeric, "ст37w" numeric, "ст38m" numeric, "ст38w" numeric, "ст39m" numeric, "ст39w" numeric, "ст40m" numeric, "ст40w" numeric, "ст41m" numeric, "ст41w" numeric, "ст42m" numeric, "ст42w" numeric, "ст43m" numeric, "ст43w" numeric, "ст44m" numeric, "ст44w" numeric, "ст45m" numeric, "ст45w" numeric, "ст46m" numeric, "ст46w" numeric, "ст47m" numeric, "ст47w" numeric, "ст48m" numeric, "ст48w" numeric, "ст49m" numeric, "ст49w" numeric, "ст50m" numeric, "ст50w" numeric, "ст51m" numeric, "ст51w" numeric, "ст52m" numeric, "ст52w" numeric, "ст53m" numeric, "ст53w" numeric, "ст54m" numeric, "ст54w" numeric, "ст55m" numeric, "ст55w" numeric, "ст56m" numeric, "ст56w" numeric, "ст57m" numeric, "ст57w" numeric, "ст58m" numeric, "ст58w" numeric, "ст59m" numeric, "ст59w" numeric, "ст60m" numeric, "ст60w" numeric, "ст61m" numeric, "ст61w" numeric, "ст62m" numeric, "ст62w" numeric, "ст63m" numeric, "ст63w" numeric, "ст64m" numeric, "ст64w" numeric, "ст65m" numeric, "ст65w" numeric, "ст66m" numeric, "ст66w" numeric, "ст67m" numeric, "ст67w" numeric, "ст68m" numeric, "ст68w" numeric, "ст69m" numeric, "ст69w" numeric, "ст70m" numeric, "ст70w" numeric, "ст71m" numeric, "ст71w" numeric, "ст72m" numeric, "ст72w" numeric, "ст73m" numeric, "ст73w" numeric, "ст74m" numeric, "ст74w" numeric, "ст75m" numeric, "ст75w" numeric, "ст76m" numeric, "ст76w" numeric, "ст77m" numeric, "ст77w" numeric, "ст78m" numeric, "ст78w" numeric, "ст79m" numeric, "ст79w" numeric, "ст80m" numeric, "ст80w" numeric, "ст81m" numeric, "ст81w" numeric, "ст82m" numeric, "ст82w" numeric, "ст83m" numeric, "ст83w" numeric, "ст84m" numeric, "ст84w" numeric, "ст85m" numeric, "ст85w" numeric, "ст86m" numeric, "ст86w" numeric, "ст87m" numeric, "ст87w" numeric, "ст88m" numeric, "ст88w" numeric, "ст89m" numeric, "ст89w" numeric, "ст90m" numeric, "ст90w" numeric, "ст91m" numeric, "ст91w" numeric, "ст92m" numeric, "ст92w" numeric, "ст93m" numeric, "ст93w" numeric, "ст94m" numeric, "ст94w" numeric, "ст95m" numeric, "ст95w" numeric, "ст96m" numeric, "ст96w" numeric, "ст97m" numeric, "ст97w" numeric, "ст98m" numeric, "ст98w" numeric, "ст99m" numeric, "ст99w" numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

-- Отчет "ПФ. Профилактический медицинский осмотр (Подотчет)"

--DROP FUNCTION ks_ddlcontrol.f_3490


--do $$

declare

--v_nmode integer DEFAULT 0;
--v_ckiguid character varying DEFAULT ''::character varying;
--v_Дата text default '20201231' ;
--v_doc_id text default '3' ;

    v_sel text;
v_seldel text;
v_sel1 text;
v_temp_result varchar(254);
v_table_Услуги character varying default null;
v_table_ВозрастМ character varying default null;
v_table_ВозрастЖ character varying default null;
v_tmp_guid character varying default null;
v_msg_text text;
v_except_detail text;
v_except_hint text;
v_i integer;
v_j integer;
v_part_tbl text;
begin

if v_nmode=-1 then return;
end if;
v_tmp_guid := replace(cast(dbo.sys_guid() as varchar(36)),'-','_');
v_table_Услуги := concat('Запрос_51976900_',v_tmp_guid);
v_table_ВозрастМ := concat('Запрос_52619886_',v_tmp_guid);
v_table_ВозрастЖ := concat('Запрос_52639949_',v_tmp_guid);
v_seldel := '
drop table if exists ' || v_table_Услуги || ';
drop table if exists ' || v_table_ВозрастМ || ';
drop table if exists ' || v_table_ВозрастЖ || ';';
begin 
	perform ks_ddlcontrol.sp_ds_query_7224( v_nmode := v_nmode, v_table_name_result := v_table_Услуги, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_doc_id := v_doc_id);
perform ks_ddlcontrol.sp_ds_query_7225( v_nmode := v_nmode, v_table_name_result := v_table_ВозрастМ, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_doc_id := v_doc_id);
perform ks_ddlcontrol.sp_ds_query_7226( v_nmode := v_nmode, v_table_name_result := v_table_ВозрастЖ, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_doc_id := v_doc_id);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
RAISE EXCEPTION 'ошибка получения исходных данных: %', v_msg_text
      USING HINT = 'Проверьте используемые запросы.';
end;
v_i := 18;
v_part_tbl := '';
while (v_i <= 99) loop
	v_part_tbl := v_part_tbl || ', ст'||right('00'||(v_i::text),2)||'m numeric(22,2) null';
v_part_tbl := v_part_tbl || ', ст'||right('00'||(v_i::text),2)||'w numeric(22,2) null';
v_i := v_i + 1;
end loop;
v_temp_result := concat('temp_result_',v_tmp_guid);
v_sel := '

drop table if exists ' || v_temp_result || ';

create temporary table ' || v_temp_result || '
(
	ord varchar(10)
	, Вид text
 	, услуга_id integer
 	, Услуга text
 	, Тариф numeric(22,2) null
 	, РайонКоэф numeric(22,2) null
	' || v_part_tbl || '
) on commit drop;

insert into ' || v_temp_result || ' (ord, Вид, услуга_id, Услуга)
values(''00'', ''Проф. мед. осмотр'', 0, ''ТАРИФ профилактического медицинского осмотра, рублей'') ;

insert into ' || v_temp_result || ' (ord, Вид, услуга_id, Услуга, РайонКоэф)
select 
''00_0'',''Проф. мед. осмотр'', -1, ''ТАРИФ профилактического медицинского осмотра приведенный РА, рублей'', районный_коэф 
from ' || v_table_Услуги || '
limit 1;

insert into ' || v_temp_result || ' (ord, Вид, услуга_id, Услуга, Тариф)
select 
right(''00''||cast(dense_rank() over(order by услуга_id) as varchar), 2) as ord,
''Проф. мед. осмотр'',
услуга_id, услуга_наименование, тариф
from ' || v_table_Услуги || ';

create temporary table t_ВозрастМ on commit drop as 
select услуга_id as id, возраст, стоимость_м as стоимость from ' || v_table_ВозрастМ || ';

create temporary table t_ВозрастЖ on commit drop as 
select услуга_id as id, возраст, стоимость_ж as стоимость from ' || v_table_ВозрастЖ || ';

create temporary table t_tmp on commit drop as 
select * from ' || v_temp_result || ';

';
begin 
	execute (v_sel);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
execute (v_seldel);
RAISE EXCEPTION 'ошибка формирования результата отчета: %', coalesce(v_msg_text,'')
      USING HINT = v_sel;
end;
with s1 as (
	select 
		 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'m = '||cast(стоимость as varchar)||' where услуга_id = '||cast(id as varchar)||';'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'m = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'m,0)+'||cast(стоимость as varchar)||' where услуга_id = 0;'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'m = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'m,0)+'||cast(стоимость as varchar)||'*РайонКоэф where услуга_id = -1;' as sel		
	from t_ВозрастМ
	union all
	select 
		'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'w = '||cast(стоимость as varchar)||' where услуга_id = '||cast(id as varchar)||';'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'w = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'w,0)+'||cast(стоимость as varchar)||' where услуга_id = 0;'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'w = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'w,0)+'||cast(стоимость as varchar)||'*РайонКоэф where услуга_id = -1;' as sel
	from t_ВозрастЖ
)
select 
string_agg(sel, chr(10))
into v_sel
from s1;
execute (v_sel);
v_sel = 'update t_tmp set услуга_id = 0 where услуга_id = -1;';
execute (v_sel);
execute (v_seldel);
-- Финальный процесс
if coalesce(v_table_name_result,'') <> '' then

      v_sel := concat('drop table if exists ', v_table_name_result,';create temporary table ', v_table_name_result , ' on commit drop as select  * from t_tmp order by ord ');
execute v_sel;
else

      v_sel := concat('select  * from t_tmp order by ord  ');
return query execute v_sel;
end if;
end;
--$$
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3494(v_nmode integer DEFAULT 0, v_table_name_result character varying DEFAULT ''::character varying, v_ckiguid character varying DEFAULT ''::character varying, "v_Дата" text DEFAULT NULL::text, v_doc_id text DEFAULT NULL::text)
 RETURNS TABLE(ord character varying, "услуга__id" integer, "Услуга" text, "Тариф" numeric, "РайонКоэф" numeric, "ст18m" numeric, "ст18w" numeric, "ст19m" numeric, "ст19w" numeric, "ст20m" numeric, "ст20w" numeric, "ст21m" numeric, "ст21w" numeric, "ст22m" numeric, "ст22w" numeric, "ст23m" numeric, "ст23w" numeric, "ст24m" numeric, "ст24w" numeric, "ст25m" numeric, "ст25w" numeric, "ст26m" numeric, "ст26w" numeric, "ст27m" numeric, "ст27w" numeric, "ст28m" numeric, "ст28w" numeric, "ст29m" numeric, "ст29w" numeric, "ст30m" numeric, "ст30w" numeric, "ст31m" numeric, "ст31w" numeric, "ст32m" numeric, "ст32w" numeric, "ст33m" numeric, "ст33w" numeric, "ст34m" numeric, "ст34w" numeric, "ст35m" numeric, "ст35w" numeric, "ст36m" numeric, "ст36w" numeric, "ст37m" numeric, "ст37w" numeric, "ст38m" numeric, "ст38w" numeric, "ст39m" numeric, "ст39w" numeric, "ст40m" numeric, "ст40w" numeric, "ст41m" numeric, "ст41w" numeric, "ст42m" numeric, "ст42w" numeric, "ст43m" numeric, "ст43w" numeric, "ст44m" numeric, "ст44w" numeric, "ст45m" numeric, "ст45w" numeric, "ст46m" numeric, "ст46w" numeric, "ст47m" numeric, "ст47w" numeric, "ст48m" numeric, "ст48w" numeric, "ст49m" numeric, "ст49w" numeric, "ст50m" numeric, "ст50w" numeric, "ст51m" numeric, "ст51w" numeric, "ст52m" numeric, "ст52w" numeric, "ст53m" numeric, "ст53w" numeric, "ст54m" numeric, "ст54w" numeric, "ст55m" numeric, "ст55w" numeric, "ст56m" numeric, "ст56w" numeric, "ст57m" numeric, "ст57w" numeric, "ст58m" numeric, "ст58w" numeric, "ст59m" numeric, "ст59w" numeric, "ст60m" numeric, "ст60w" numeric, "ст61m" numeric, "ст61w" numeric, "ст62m" numeric, "ст62w" numeric, "ст63m" numeric, "ст63w" numeric, "ст64m" numeric, "ст64w" numeric, "ст65m" numeric, "ст65w" numeric, "ст66m" numeric, "ст66w" numeric, "ст67m" numeric, "ст67w" numeric, "ст68m" numeric, "ст68w" numeric, "ст69m" numeric, "ст69w" numeric, "ст70m" numeric, "ст70w" numeric, "ст71m" numeric, "ст71w" numeric, "ст72m" numeric, "ст72w" numeric, "ст73m" numeric, "ст73w" numeric, "ст74m" numeric, "ст74w" numeric, "ст75m" numeric, "ст75w" numeric, "ст76m" numeric, "ст76w" numeric, "ст77m" numeric, "ст77w" numeric, "ст78m" numeric, "ст78w" numeric, "ст79m" numeric, "ст79w" numeric, "ст80m" numeric, "ст80w" numeric, "ст81m" numeric, "ст81w" numeric, "ст82m" numeric, "ст82w" numeric, "ст83m" numeric, "ст83w" numeric, "ст84m" numeric, "ст84w" numeric, "ст85m" numeric, "ст85w" numeric, "ст86m" numeric, "ст86w" numeric, "ст87m" numeric, "ст87w" numeric, "ст88m" numeric, "ст88w" numeric, "ст89m" numeric, "ст89w" numeric, "ст90m" numeric, "ст90w" numeric, "ст91m" numeric, "ст91w" numeric, "ст92m" numeric, "ст92w" numeric, "ст93m" numeric, "ст93w" numeric, "ст94m" numeric, "ст94w" numeric, "ст95m" numeric, "ст95w" numeric, "ст96m" numeric, "ст96w" numeric, "ст97m" numeric, "ст97w" numeric, "ст98m" numeric, "ст98w" numeric, "ст99m" numeric, "ст99w" numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

-- Отчет "ПФ 2 этап диспансеризации (Подотчет)"

--do $$

declare
/*
v_nmode integer DEFAULT 0;
 v_ckiguid character varying DEFAULT ''::character varying;
v_Дата text default '20201231' ;
v_doc_id text default '2' ;
*/
    v_sel text;
	v_seldel text;
   	v_sel1 text;
    v_temp_result varchar(254);
    v_table_Услуги character varying default null;
    v_table_ВозрастМ character varying default null;
	v_table_ВозрастЖ character varying default null;
	v_tmp_guid character varying default null;
	v_msg_text text;
	v_except_detail text;
	v_except_hint text;

	v_i integer;
	v_j integer;
	v_part_tbl text;


begin

if (v_nmode = -1) then
	return;
end if;
	
v_tmp_guid := replace(cast(dbo.sys_guid() as varchar(36)),'-','_');

v_table_Услуги := concat('Запрос_63093249_',v_tmp_guid);
v_table_ВозрастМ := concat('Запрос_63442787_',v_tmp_guid);
v_table_ВозрастЖ := concat('Запрос_63574416_',v_tmp_guid);

v_seldel := '
drop table if exists ' || v_table_Услуги || ';
drop table if exists ' || v_table_ВозрастМ || ';
drop table if exists ' || v_table_ВозрастЖ || ';';

begin 
	perform ks_ddlcontrol.sp_ds_query_7239( v_nmode := v_nmode, v_table_name_result := v_table_Услуги, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_doc_id := v_doc_id);
	perform ks_ddlcontrol.sp_ds_query_7238( v_nmode := v_nmode, v_table_name_result := v_table_ВозрастМ, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_doc_id := v_doc_id);
	perform ks_ddlcontrol.sp_ds_query_7240( v_nmode := v_nmode, v_table_name_result := v_table_ВозрастЖ, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_doc_id := v_doc_id);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	execute (v_seldel);
	RAISE EXCEPTION 'ошибка получения исходных данных: %', v_msg_text
      USING HINT = 'Проверьте используемые запросы.';

end; 

v_i := 18;
v_part_tbl := '';

while (v_i <= 99) loop
	v_part_tbl := v_part_tbl || ', ст'||right('00'||(v_i::text),2)||'m numeric(22,2) null';	
	v_part_tbl := v_part_tbl || ', ст'||right('00'||(v_i::text),2)||'w numeric(22,2) null';	
	v_i := v_i + 1;
end loop;

v_temp_result := concat('temp_result_',v_tmp_guid);

v_sel := '

drop table if exists ' || v_temp_result || ';

create temporary table ' || v_temp_result || '
(
	ord varchar(10)
 	, услуга__id integer
 	, Услуга text
 	, Тариф numeric(22,2) null
 	, РайонКоэф numeric(22,2) null
	' || v_part_tbl || '
) on commit drop;

insert into ' || v_temp_result || ' (ord, услуга__id, Услуга)
values(''00'', 0, ''ТАРИФ первого этапа диспансеризации, рублей'') ;

insert into ' || v_temp_result || ' (ord, услуга__id, Услуга, РайонКоэф)
select 
''00_0'', -1, ''ТАРИФ первого этапа диспансеризации приведенный РА, рублей'', районный_коэф_  
from ' || v_table_Услуги || '
limit 1;

insert into ' || v_temp_result || ' (ord, услуга__id, Услуга, Тариф)
select 
right(''00''||cast(dense_rank() over(order by услуга__id) as varchar), 2) as ord,
услуга__id, услуга_наименование, услуга_тариф
from ' || v_table_Услуги || ';

create temporary table t_ВозрастМ on commit drop as 
select услуга__id as id, возраст, стоимость from ' || v_table_ВозрастМ || ';

create temporary table t_ВозрастЖ on commit drop as 
select услуга__id as id, возраст, стоимость from ' || v_table_ВозрастЖ || ';

create temporary table t_tmp on commit drop as 
select * from ' || v_temp_result || ';

';

begin 
	execute (v_sel);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	execute (v_seldel);
	RAISE EXCEPTION 'ошибка формирования результата отчета: %', coalesce(v_msg_text,'')
      USING HINT = v_sel;

end; 

with s1 as (
	select 
		 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'m = '||cast(стоимость as varchar)||' where услуга__id = '||cast(id as varchar)||';'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'m = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'m,0)+'||cast(стоимость as varchar)||' where услуга__id = 0;'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'m = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'m,0)+'||cast(стоимость as varchar)||'*РайонКоэф where услуга__id = -1;' as sel		
	from t_ВозрастМ
	union all
	select 
		'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'w = '||cast(стоимость as varchar)||' where услуга__id = '||cast(id as varchar)||';'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'w = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'w,0)+'||cast(стоимость as varchar)||' where услуга__id = 0;'
	  || 'update t_tmp set ст'||right('00'||cast(возраст as varchar),2)||'w = coalesce(ст'||right('00'||cast(возраст as varchar),2)||'w,0)+'||cast(стоимость as varchar)||'*РайонКоэф where услуга__id = -1;' as sel
	from t_ВозрастЖ
)
select 
string_agg(sel, chr(10))
into v_sel
from s1;

execute (v_sel);

v_sel = 'update t_tmp set услуга__id = 0 where услуга__id = -1;';
execute (v_sel);

execute (v_seldel);

-- Финальный процесс
if coalesce(v_table_name_result,'') <> '' then

      v_sel := concat('drop table if exists ', v_table_name_result,';create temporary table ', v_table_name_result , ' on commit drop as select  * from t_tmp order by ord ');
      execute v_sel;

else

      v_sel := concat('select  * from t_tmp order by ord  ');
      return query execute v_sel;

end if;

end;
--$$
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3507_fill_cnt_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

--do $$
-- МИД 3507 Актуализации информации в реестре МО о лицензиях на медицинскую деятельность  
-- Цель: заполнить атрибуты Кол-во МО и Кол-во лицензий по кнопке Действия «Заполнить количество МО и лицензий» 

declare
--v_doc_id integer;
v_Unit uuid;

begin 

--v_doc_id := 2;

select dbo.sys_guid() 
into v_Unit;
-- select * from KS_DDLControl.d001_sp where documentid = 3991  


create temporary table t_d on commit drop as -- текущий документ
select 
d.id,
at_141105 as тДата,
at_141111 as Кол_во_МО,
at_141112 as Кол_во_лицензий
from KS_DDLControl.ds_3507_23485 as d -- МИД 3507 Актуализации информации в реестре МО о лицензиях на медицинскую деятельность
where d.id = v_doc_id
;
create temporary table t_inst1 on commit drop as
 -- Макет справочников «Единый реестр медицинских организаций, осуществляющих деятельность в сфере обязательного медицинского страхования (MO) (реестр)» (код F003 id 3059) / 
 -- справочник «F003. Единый реестр медицинских организаций, осуществляющих деятельность в сфере ОМС»
select
s.id,
mcod,
sv.date as элДата,
тДата
from t_d
inner join KS_DDLControl.f003_r as s on (t_d.тДата <= s.date_e or s.date_e is null) and left(mcod,2) = '04'
inner join KS_DDLControl.f003_r_vers as sv on sv.id_up = s.id 
	and sv.date = (
		select max(sv0.date) 
		from KS_DDLControl.f003_r_vers as sv0 
		where sv.id_up=sv0.id_up and sv0.date <= тДата)
;
create temporary table t_inst2 on commit drop as
 -- Макет справочников «Единый реестр медицинских организаций, осуществляющих деятельность в сфере обязательного медицинского страхования (MO)_d» (код F003_d id 3058) / 
 -- справочник «F003_d. Единый реестр МО, осуществляющих деятельность в сфере обязательного медицинского страхования»
select
count(distinct t1.id) as Кол_во_МО,
count(distinct st.id) as Кол_во_лицензий
from t_inst1 as t1
inner join KS_DDLControl.f003_d as s on s.mcod = t1.mcod 
	and s.d_edit = t1.элДата  -- Дата последнего редактирования записи = ТЧ Версионный. Начало (из п.1)
inner join KS_DDLControl.f003_d_doc as st on st.id_up = s.id -- ТЧ Лицензии МО на осуществление медицинской деятельности
	and (тДата <= st.date_e or st.date_e is null)  -- Шапка документа. Дата <= Дата окончания действия лицензии на осуществление деятельности МО 
	and (тДата <= st.d_term or st.d_term is null)  -- Шапка документа. Дата <= Дата досрочного прекращения действия лицензии
;

update ks_ddlcontrol.ds_3507_23485
set 
at_141111 = Кол_во_МО,
at_141112 = Кол_во_лицензий
from t_inst2
where ks_ddlcontrol.ds_3507_23485.id = v_doc_id
;


end
-- $$;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3525(v_nmode integer DEFAULT 0, v_table_name_result character varying DEFAULT ''::character varying, v_ckiguid character varying DEFAULT ''::character varying, "v_Дата" text DEFAULT NULL::text, "v_Статус" text DEFAULT NULL::text)
 RETURNS TABLE(ord character varying, "пол" text, "ст21" character varying, "ст24" character varying, "ст27" character varying, "ст30" character varying, "ст33" character varying, "ст36" character varying, "ст39" character varying, "ст42" character varying, "ст45" character varying, "ст48" character varying, "ст51" character varying, "ст54" character varying, "ст57" character varying, "ст60" character varying, "ст63" character varying, "ст66" character varying, "ст69" character varying, "ст72" character varying, "ст75" character varying, "ст78" character varying, "ст81" character varying, "ст84" character varying, "ст87" character varying, "ст90" character varying, "ст93" character varying, "ст96" character varying, "ст99" character varying)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

-- Отчет "Тарифы на диспансеризацию определенных групп взрослого населения"

--do $$

declare

v_nmode integer DEFAULT 0;
 v_ckiguid character varying DEFAULT ''::character varying;
--v_Дата text default '20201231' ;
--v_Статус text default '131' ;

    v_sel text;
	v_seldel text;
   	v_sel1 text;
    v_temp_result varchar(254);
    v_table_Тарифы character varying default null;
	v_tmp_guid character varying default null;
	v_msg_text text;
	v_except_detail text;
	v_except_hint text;

	v_i integer;
	v_j integer;
	v_part_tbl text;
	v_part_sel text;


begin

if (v_nmode = -1) then
	return;
end if;
	
v_tmp_guid := replace(cast(dbo.sys_guid() as varchar(36)),'-','_');

v_table_Тарифы := concat('Запрос_63093249_',v_tmp_guid);

v_seldel := '
drop table if exists ' || v_table_Тарифы || ';';

begin 
	perform ks_ddlcontrol.sp_ds_query_7297( v_nmode := v_nmode, v_table_name_result := v_table_Тарифы, v_ckiguid := v_ckiguid, v_Статус := v_Статус);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	execute (v_seldel);
	RAISE EXCEPTION 'ошибка получения исходных данных: %', v_msg_text
      USING HINT = 'Проверьте используемые запросы.';

end; 

v_i := 21;
v_part_tbl := '';
v_part_sel := '';

while (v_i <= 99) loop
	v_part_tbl := v_part_tbl || ', ст'||right('00'||(v_i::text),2)||' character varying DEFAULT ''''';	
	v_part_sel := v_part_sel || ', string_agg(cast( case when возраст='||(v_i::text)||' then тариф else null end as varchar(20)), chr(10))  as ст'||right('00'||(v_i::text),2);	
	v_i := v_i + 3;
end loop;

v_temp_result := concat('temp_result_',v_tmp_guid);

v_sel := '

delete from ' || v_table_Тарифы || ' where not (
	cast(год as int) = 0' || left(coalesce(v_Дата,'2021'), 4) || ' and
	cast(to_char(дата_тарифа, ''YYYYMMDD'') as int) <= 0' || left(replace(coalesce(v_Дата,'2021'), '-', ''), 8) || ' 	
);

delete from ' || v_table_Тарифы || ' where 
	дата_тарифа <> ( select max(дата_тарифа) from ' || v_table_Тарифы || ' );

drop table if exists ' || v_temp_result || ';

create temporary table ' || v_temp_result || '
(
	ord varchar(10)
 	, пол text
	' || v_part_tbl || '
) on commit drop;

insert into ' || v_temp_result || ' 
select 
cast(пол as varchar),
case when пол=1 then ''М'' else ''Ж'' end as пол
' || v_part_sel || '
from ' || v_table_Тарифы || '
group by пол;

';

begin 
	execute (v_sel);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	execute (v_seldel);
	RAISE EXCEPTION 'ошибка формирования результата отчета: %', coalesce(v_msg_text,'')
      USING HINT = v_sel;

end; 

execute (v_seldel);


-- Финальный процесс
if coalesce(v_table_name_result,'') <> '' then

      v_sel := concat('drop table if exists ', v_table_name_result,';create temporary table ', v_table_name_result , ' on commit drop as select  * from ' || v_temp_result || ' order by ord ');
      execute v_sel;

else

      v_sel := concat('select  * from ' || v_temp_result || ' order by ord  ');
      return query execute v_sel;

end if;

end;
--$$
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3953_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

declare
v_Unit uuid;
begin 

-- расчет строки 8 = 9+10		
update ks_ddlcontrol.ds_3336_21404 t
set 
at_139214= t2.at_139214,
at_139215= t2.at_139215,
at_139216= t2.at_139216,
at_139217= t2.at_139217,
at_139218= t2.at_139218,
at_139219= t2.at_139219
from (select id_up, sum(at_139214) as at_139214, sum(at_139215) as at_139215,
sum(at_139216) as at_139216, sum(at_139217) as at_139217,
sum(at_139218) as at_139218, sum(at_139219) as at_139219
from ks_ddlcontrol.ds_3336_21404 t
left join ks_ddlcontrol.ds_3336_21398 t2 on t.id_up=t2.id
where cl_139229 in (10,9) and t2.id=v_doc_id and t2.documentid IN (3953) 
group by id_up) t2
where t.cl_139229=8 and t.id_up=t2.id_up;
-- расчет строки 4 = 5+6+7, за исключеним данных их подушевного норматива	
update ks_ddlcontrol.ds_3336_21404 t
set 
at_139214= t2.at_139214,
--at_139215= t2.at_139215,
at_139216= t2.at_139216,
--at_139217= t2.at_139217,
at_139218= t2.at_139218
--at_139219= t2.at_139219
from (select id_up, sum(at_139214) as at_139214, sum(at_139215) as at_139215,
sum(at_139216) as at_139216, sum(at_139217) as at_139217,
sum(at_139218) as at_139218, sum(at_139219) as at_139219
from ks_ddlcontrol.ds_3336_21404 t
left join ks_ddlcontrol.ds_3336_21398 t2 on t.id_up=t2.id
where cl_139229 in (5,6,7) and t2.id=v_doc_id and t2.documentid IN (3953) 
group by id_up) t2
where t.cl_139229=4 and t.id_up=t2.id_up;
-- расчет строки 3 = 4+8
update ks_ddlcontrol.ds_3336_21404 t
set 
at_139214= t2.at_139214,
at_139215= t2.at_139215,
at_139216= t2.at_139216,
at_139217= t2.at_139217,
at_139218= t2.at_139218,
at_139219= t2.at_139219
from (select id_up, sum(at_139214) as at_139214, sum(at_139215) as at_139215,
sum(at_139216) as at_139216, sum(at_139217) as at_139217,
sum(at_139218) as at_139218, sum(at_139219) as at_139219
from ks_ddlcontrol.ds_3336_21404 t
left join ks_ddlcontrol.ds_3336_21398 t2 on t.id_up=t2.id
where cl_139229 in (4,8) and t2.id=v_doc_id  and t2.documentid IN (3953) 
group by id_up) t2
where t.cl_139229=3 and t.id_up=t2.id_up;
-- расчет строки 1 = 2+3
update ks_ddlcontrol.ds_3336_21404 t
set 
at_139214= t2.at_139214,
at_139215= t2.at_139215,
at_139216= t2.at_139216,
at_139217= t2.at_139217,
at_139218= t2.at_139218,
at_139219= t2.at_139219
from (select id_up, sum(at_139214) as at_139214, sum(at_139215) as at_139215,
sum(at_139216) as at_139216, sum(at_139217) as at_139217,
sum(at_139218) as at_139218, sum(at_139219) as at_139219
from ks_ddlcontrol.ds_3336_21404 t
left join ks_ddlcontrol.ds_3336_21398 t2 on t.id_up=t2.id
where cl_139229 in (2,3) and t2.id=v_doc_id  and t2.documentid IN (3953) 
group by id_up) t2
where t.cl_139229=1 and t.id_up=t2.id_up;
end;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_3966_save_(v_doc_id integer)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

declare
v_Unit uuid;
begin 

--1) обновление офг 
update ks_ddlcontrol.tab t
set ob1=t2.ob1,
sto=t2.sto
from(
select id_up, sum(ob1) as ob1,  sum(sto) as sto, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm from ks_ddlcontrol.tab as t
Right join (select
	t.id as id,
	t.chis as chis,
	t_l21503_139297.norm as norm,
	t_l21503_139297.id as таблица_показатели_стоимости_тп_id,
	t_l21503_139297_l21980_139740.id as подстроки_показатели_id
 from
	((((ks_ddlcontrol.tp010 t
		LEFT OUTER JOIN ks_ddlcontrol.tab t_l21503 ON (t.id = t_l21503.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297 ON (t_l21503.tpp008 = t_l21503_139297.id))
		LEFT OUTER JOIN ks_ddlcontrol.podstroka t_l21503_139297_l21980 ON (t_l21503_139297.id = t_l21503_139297_l21980.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297_l21980_139740 ON (t_l21503_139297_l21980.tpp008 = t_l21503_139297_l21980_139740.id))
where
	t.documentid IN (3966) and t_l21503_139297.gr=1 and t_l21503_139297.ras=1 ) as t2 on t.tpp008=t2.подстроки_показатели_id
where t.id_up=t2.id
group by id_up, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm) as t2
where t.id_up=v_doc_id and t.id_up=t2.id and t.tpp008=t2.таблица_показатели_стоимости_тп_id;
-- 2) обновление офг1_2 расчет формул для групповых
update ks_ddlcontrol.tab t
set 
normoms=t2.ob1*t2.sto,
stooms=(round(t2.ob1*t2.sto, 2)*t2.chis)/1000
from(
select id_up, sum(ob1) as ob1,  sum(sto) as sto, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm from ks_ddlcontrol.tab as t
Right join (select
	t.id as id,
	t.chis as chis,
	t_l21503_139297.norm as norm,
	t_l21503_139297.id as таблица_показатели_стоимости_тп_id,
	t_l21503_139297_l21980_139740.id as подстроки_показатели_id
 from
	((((ks_ddlcontrol.tp010 t
		LEFT OUTER JOIN ks_ddlcontrol.tab t_l21503 ON (t.id = t_l21503.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297 ON (t_l21503.tpp008 = t_l21503_139297.id))
		LEFT OUTER JOIN ks_ddlcontrol.podstroka t_l21503_139297_l21980 ON (t_l21503_139297.id = t_l21503_139297_l21980.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297_l21980_139740 ON (t_l21503_139297_l21980.tpp008 = t_l21503_139297_l21980_139740.id))
where
	t.documentid IN (3966) and t_l21503_139297.gr=1 and t_l21503_139297.ras=1 and t_l21503_139297.norm<>1) as t2 on t.tpp008=t2.подстроки_показатели_id
where t.id_up=t2.id
group by id_up, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm) as t2
where t.id_up=v_doc_id and t.id_up=t2.id and t.tpp008=t2.таблица_показатели_стоимости_тп_id;
-- Очистко колонок 4,5,6 офг
update ks_ddlcontrol.tab
set ob1 = null, ob = null, sto = null
from ks_ddlcontrol.index_price t2 
where id_up = v_doc_id and t2.ras=0 and tpp008 = t2.id;
-- Очистко колонок 8,10 офг
update ks_ddlcontrol.tab
set normoms = null, stooms = null
from ks_ddlcontrol.index_price t2 
where id_up = v_doc_id and tpp008 = t2.id and t2.ras=0 and t2.ruch=0 and t2.norm=0;
-- Очистко колонок 11 офг
update ks_ddlcontrol.tab
set itog = null
from ks_ddlcontrol.index_price t2 
where id_up = v_doc_id and tpp008 = t2.id and t2.itog=0;
-- 3) обновление пфг1
update ks_ddlcontrol.tab1 t
set 
ob1_1=t2.ob1,
sto_1=t2.sto
from(
select id_up, sum(ob1_1) as ob1,  sum(sto_1) as sto, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm from ks_ddlcontrol.tab1 as t
Right join (select
	t.id as id,
	t.chis as chis,
	t_l21503_139297.norm as norm,
	t_l21503_139297.id as таблица_показатели_стоимости_тп_id,
	t_l21503_139297_l21980_139740.id as подстроки_показатели_id
 from
	((((ks_ddlcontrol.tp010 t
		LEFT OUTER JOIN ks_ddlcontrol.tab1 t_l21503 ON (t.id = t_l21503.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297 ON (t_l21503.tpp008_1 = t_l21503_139297.id))
		LEFT OUTER JOIN ks_ddlcontrol.podstroka t_l21503_139297_l21980 ON (t_l21503_139297.id = t_l21503_139297_l21980.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297_l21980_139740 ON (t_l21503_139297_l21980.tpp008 = t_l21503_139297_l21980_139740.id))
where
	t.documentid IN (3966) and t_l21503_139297.gr=1 and t_l21503_139297.ras=1 ) as t2 on t.tpp008_1=t2.подстроки_показатели_id
where t.id_up=t2.id
group by id_up, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm ) as t2
where t.id_up=v_doc_id and t.id_up=t2.id and t.tpp008_1=t2.таблица_показатели_стоимости_тп_id;
-- 4) обновление офг1_2 расчет формул для групповых
update ks_ddlcontrol.tab1 t
set 
normoms_1=t2.ob1*t2.sto,
stooms_1=(round(t2.ob1*t2.sto, 2)*t2.chis)/1000
from(
select id_up, sum(ob1_1) as ob1,  sum(sto_1) as sto, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm from ks_ddlcontrol.tab1 as t
Right join (select
	t.id as id,
	t.chis as chis,
	t_l21503_139297.norm as norm,
	t_l21503_139297.id as таблица_показатели_стоимости_тп_id,
	t_l21503_139297_l21980_139740.id as подстроки_показатели_id
 from
	((((ks_ddlcontrol.tp010 t
		LEFT OUTER JOIN ks_ddlcontrol.tab1 t_l21503 ON (t.id = t_l21503.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297 ON (t_l21503.tpp008_1 = t_l21503_139297.id))
		LEFT OUTER JOIN ks_ddlcontrol.podstroka t_l21503_139297_l21980 ON (t_l21503_139297.id = t_l21503_139297_l21980.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297_l21980_139740 ON (t_l21503_139297_l21980.tpp008 = t_l21503_139297_l21980_139740.id))
where
	t.documentid IN (3966) and t_l21503_139297.gr=1 and t_l21503_139297.ras=1 and t_l21503_139297.norm<>1) as t2 on t.tpp008_1=t2.подстроки_показатели_id
where t.id_up=t2.id
group by id_up, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm ) as t2
where t.id_up=v_doc_id and t.id_up=t2.id and t.tpp008_1=t2.таблица_показатели_стоимости_тп_id;
-- Очистко колонок 4,5,6 пфг1
update ks_ddlcontrol.tab1
set ob1_1 = null, ob_1 = null, sto_1 = null
from ks_ddlcontrol.index_price t2 
where id_up = v_doc_id and t2.ras=0 and tpp008_1 = t2.id;
-- Очистко колонок 8,10 пфг1
update ks_ddlcontrol.tab1
set normoms_1 = null, stooms_1 = null
from ks_ddlcontrol.index_price t2 
where id_up = v_doc_id and tpp008_1 = t2.id and t2.ras=0 and t2.ruch=0 and t2.norm=0;
-- Очистко колонок 11 пфг1
update ks_ddlcontrol.tab1
set itog_1 = null
from ks_ddlcontrol.index_price t2 
where id_up = v_doc_id and tpp008_1 = t2.id and t2.itog=0;
-- 5) обновление пфг2
update ks_ddlcontrol.tab2 t
set ob1_2=t2.ob1,
sto_2=t2.sto
from(
select id_up, sum(ob1_2) as ob1,  sum(sto_2) as sto, таблица_показатели_стоимости_тп_id, t2.id, t2.chis,t2.norm from ks_ddlcontrol.tab2 as t
Right join (select
	t.id as id,
	t.chis as chis,
	t_l21503_139297.norm as norm,
	t_l21503_139297.id as таблица_показатели_стоимости_тп_id,
	t_l21503_139297_l21980_139740.id as подстроки_показатели_id
 from
	((((ks_ddlcontrol.tp010 t
		LEFT OUTER JOIN ks_ddlcontrol.tab2 t_l21503 ON (t.id = t_l21503.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297 ON (t_l21503.tpp008_2 = t_l21503_139297.id))
		LEFT OUTER JOIN ks_ddlcontrol.podstroka t_l21503_139297_l21980 ON (t_l21503_139297.id = t_l21503_139297_l21980.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297_l21980_139740 ON (t_l21503_139297_l21980.tpp008 = t_l21503_139297_l21980_139740.id))
where
	t.documentid IN (3966) and t_l21503_139297.gr=1 and t_l21503_139297.ras=1 ) as t2 on t.tpp008_2=t2.подстроки_показатели_id
where t.id_up=t2.id
group by id_up, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm ) as t2
where t.id_up=v_doc_id and t.id_up=t2.id and t.tpp008_2=t2.таблица_показатели_стоимости_тп_id;
--6) обновление офг1_2 расчет формул для групповых
update ks_ddlcontrol.tab2 t
set 
normoms_2=t2.ob1*t2.sto,
stooms_2=(round(t2.ob1*t2.sto, 2)*t2.chis)/1000
from(
select id_up, sum(ob1_2) as ob1,  sum(sto_2) as sto, таблица_показатели_стоимости_тп_id, t2.id, t2.chis,t2.norm from ks_ddlcontrol.tab2 as t
Right join (select
	t.id as id,
	t.chis as chis,
	t_l21503_139297.norm as norm,
	t_l21503_139297.id as таблица_показатели_стоимости_тп_id,
	t_l21503_139297_l21980_139740.id as подстроки_показатели_id
 from
	((((ks_ddlcontrol.tp010 t
		LEFT OUTER JOIN ks_ddlcontrol.tab2 t_l21503 ON (t.id = t_l21503.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297 ON (t_l21503.tpp008_2 = t_l21503_139297.id))
		LEFT OUTER JOIN ks_ddlcontrol.podstroka t_l21503_139297_l21980 ON (t_l21503_139297.id = t_l21503_139297_l21980.id_up))
		LEFT OUTER JOIN ks_ddlcontrol.index_price t_l21503_139297_l21980_139740 ON (t_l21503_139297_l21980.tpp008 = t_l21503_139297_l21980_139740.id))
where
	t.documentid IN (3966) and t_l21503_139297.gr=1 and t_l21503_139297.ras=1 and t_l21503_139297.norm<>1) as t2 on t.tpp008_2=t2.подстроки_показатели_id
where t.id_up=t2.id
group by id_up, таблица_показатели_стоимости_тп_id, t2.id, t2.chis, t2.norm ) as t2
where t.id_up=v_doc_id and t.id_up=t2.id and t.tpp008_2=t2.таблица_показатели_стоимости_тп_id;
-- Очистко колонок 4,5,6 пфг2
update ks_ddlcontrol.tab2
set ob1_2 = null, ob_2 = null, sto_2 = null
from ks_ddlcontrol.index_price t2 
where id_up = v_doc_id and t2.ras=0 and tpp008_2 = t2.id;
-- Очистко колонок 8,10 пфг2
update ks_ddlcontrol.tab2
set normoms_2 = null, stooms_2 = null
from ks_ddlcontrol.index_price t2 
where id_up = v_doc_id and tpp008_2 = t2.id and t2.ras=0 and t2.ruch=0 and t2.norm=0;
-- Очистко колонок 11 пфг2
update ks_ddlcontrol.tab2
set itog_2 = null
from ks_ddlcontrol.index_price t2 
where id_up = v_doc_id and tpp008_2 = t2.id and t2.itog=0;
end;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_5488(v_nmode integer DEFAULT 0, v_table_name_result character varying DEFAULT ''::character varying, v_ckiguid character varying DEFAULT ''::character varying, "v_Статус" text DEFAULT '___Все___'::text, "v_Дата" text DEFAULT '20191201'::text, "v_ППО" text DEFAULT '17'::text, "v_Версия" text DEFAULT '1'::text, "v_ГП" text DEFAULT '716'::text, "v_ППГП" text DEFAULT '___Все___'::text, "v_ЕдИзм" text DEFAULT '1000'::text, "v_Округление" character varying DEFAULT '0'::character varying)
 RETURNS TABLE(ord character varying, "Показатель" text, "Значение" text)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$

-- Отчет "Паспорт ППГП"

declare
    v_sel text;
	v_seldel text;
   	v_sel1 text;
    v_temp_result varchar(254);
    v_table_ГП character varying default null;
    v_table_ППГП character varying default null;
	v_table_ОМ character varying default null;
	v_table_ЦП character varying default null;
	v_table_РО character varying default null;
	v_tmp_guid character varying default null;
	v_msg_text text;
	v_except_detail text;
	v_except_hint text;

	v_dtY varchar(4);
	v_dt varchar(8);
	v_ЕдИзмНаим text;
	v_sSum text;

begin

v_tmp_guid := replace(cast(dbo.sys_guid() as varchar(36)),'-','_');

v_table_ППГП := concat('Запрос_38292108_',v_tmp_guid);
v_table_ОМ := concat('Запрос_48920111_',v_tmp_guid);
v_table_ЦП := concat('Запрос_50335360_',v_tmp_guid);
v_table_РО := concat('Запрос_34043888_',v_tmp_guid);

v_seldel := '
drop table if exists ' || v_table_ППГП || ';
drop table if exists ' || v_table_ОМ || ';
drop table if exists ' || v_table_ЦП || ';
drop table if exists ' || v_table_РО || ';';

begin 
	perform ks_ddlcontrol.sp_ds_query_3664( v_nmode := v_nmode, v_table_name_result := v_table_ППГП, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_ППО := v_ППО, v_Версия := v_Версия, v_ГП := v_ГП, v_Статус := v_Статус);
	perform ks_ddlcontrol.sp_ds_query_3666( v_nmode := v_nmode, v_table_name_result := v_table_ОМ, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_ППО := v_ППО, v_Версия := v_Версия, v_Статус := v_Статус);
	perform ks_ddlcontrol.sp_ds_query_3667( v_nmode := v_nmode, v_table_name_result := v_table_ЦП, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_ППО := v_ППО, v_Версия := v_Версия, v_ППГП := v_ППГП, v_Статус := v_Статус);
	perform ks_ddlcontrol.sp_ds_query_3668( v_nmode := v_nmode, v_table_name_result := v_table_РО, v_ckiguid := v_ckiguid, v_Дата := v_Дата, v_ППО := v_ППО, v_Версия := v_Версия, v_Статус := v_Статус);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	RAISE EXCEPTION 'ошибка получения исходных данных: %', v_msg_text
      USING HINT = 'Проверьте используемые запросы.';

end; 


v_temp_result := concat('temp_result_',v_tmp_guid);

v_dtY := left(replace(v_Дата,'-',''),4);
v_dt := left(replace(v_Дата,'-',''),8);

v_ЕдИзмНаим := case when v_ЕдИзм='1000' then ' тыс.' when v_ЕдИзм='1000000' then ' млн.' else '' end || ' рублей';
v_sSum := 'replace(cast( (case when ' || v_Округление ||  ' = 1 then round(sum(ба) / '||v_ЕдИзм||', 2) else sum(ба) end) as varchar)  ,''.'','','') || '' ' ||v_ЕдИзмНаим||'''' ;


v_sel := '

--drop table if exists ' || v_temp_result || '; 
create temporary table ' || v_temp_result || ' (
	ord varchar(500),
	Показатель text,
	Значение text
) on commit drop;

delete from ' || v_table_ППГП || ' where  not (
	cast(to_char(дата, ''YYYY'') as int) = 0' || v_dtY || ' and 
	cast(to_char(дата, ''YYYYMMDD'') as int) <= 0' || v_dt || ' 
and 
	(''___Все___''=''' || coalesce(v_ППГП,'') || ''' OR cast(ппгп_id as varchar) IN (select * from unnest(string_to_array(''' || coalesce(v_ППГП,'') || ''', ''@#$'')) )) 
);

delete from ' || v_table_ППГП || ' where дата <> (
	select max(t.дата) 
	from   ' || v_table_ППГП || ' as t 
	where ' || v_table_ППГП || '.ппгп_id = t.ппгп_id
);

delete from ' || v_table_ОМ || ' where  not (
	cast(to_char(дата, ''YYYY'') as int) = 0' || v_dtY || ' and 
	cast(to_char(дата, ''YYYYMMDD'') as int) <= 0' || v_dt || ' and
	ппгп_id in (select ппгп_id from ' || v_table_ППГП || ') and
	ом_id is not null
);


delete from ' || v_table_ОМ || ' where дата <> (
	select max(t.дата) 
	from   ' || v_table_ОМ || ' as t 
	where ' || v_table_ОМ || '.ппгп_id = t.ппгп_id and ' || v_table_ОМ || '.грбс_id = t.грбс_id
);


delete from ' || v_table_ЦП || ' where  not (
	cast(to_char(дата, ''YYYY'') as int) = 0' || v_dtY || ' and 
	cast(to_char(дата, ''YYYYMMDD'') as int) <= 0' || v_dt || ' 
);


delete from ' || v_table_ЦП || ' where дата <> (
	select max(t.дата) 
	from   ' || v_table_ЦП || ' as t 
	where ' || v_table_ЦП || '.ппгп_id = t.ппгп_id
);

delete from ' || v_table_РО || ' where  not (
	cast(to_char(дата, ''YYYY'') as int) = 0' || v_dtY || ' and 
	cast(to_char(дата, ''YYYYMMDD'') as int) <= 0' || v_dt || ' and
	ом_id in (select ом_id from ' || v_table_ОМ || ')
);

delete from ' || v_table_РО || ' where дата <> (
	select max(t.дата) 
	from   ' || v_table_РО || ' as t 
	where ' || v_table_РО || '.ом_id = t.ом_id
);

create temporary table tППГП on commit drop as 
select distinct
right(''00'' || cast(DENSE_RANK() OVER(ORDER BY case when ппгп_тип=''Подпрограмма ГП (обеспечивающая)'' then 1 else 2 end, ппгп_порядок, ппгп_код) as varchar),2) || ''_'' as nn,
cast(DENSE_RANK() OVER(ORDER BY case when ппгп_тип=''Подпрограмма ГП (обеспечивающая)'' then 1 else 2 end, ппгп_порядок, ппгп_код) as varchar) as n,
гп_id,
гп_наименование,
ппгп_id,
ппгп_код,
ппгп_тип,
ппгп_порядок,
ппгп_наименование,
to_char(срокреализациис, ''DD.MM.YYYY'') as срокреализациис,
to_char(срокреализациипо, ''DD.MM.YYYY'') as срокреализациипо,
to_char(срокреализациипо, ''YYYY'') as год_по,
цель_пп_гп_id,
цель_пп_гп_порядок,
цель_пп_гп_наименование,
грбс_id,
грбс_код,
грбс_наименование
from ' || v_table_ППГП || ';

create temporary table tОМ on commit drop as 
select distinct
ппгп_id,
ом_id,
ом_код,
ом_наименование,
цельом_id,
цельом_код,
цельом_порядок,
цельом_наименование,
грбс_id,
грбс_код,
грбс_наименование
from ' || v_table_ОМ || ';

create temporary table tЦП on commit drop as
select 
dense_rank() over(partition by ппгп_id order by цп_порядок, цп_наименование) as num,
ппгп_id,
цп_id,
цп_едизм,
цп_едизм_код,
цп_наименование,
цп_период as год,
цп_период || '' год - '' || ks_ddlcontrol.f_trunc_numeric(цп_значение) as цп_значение
from ' || v_table_ЦП || '
order by цп_период;

create temporary table t_years on commit drop as 
select cast(y as varchar) as y
from generate_series(
	(select cast(extract(year from min(срокреализациис)) as int) from ' || v_table_ППГП || '),
	(select cast(extract(year from max(срокреализациипо)) as int) from ' || v_table_ППГП || ')) as y;

create temporary table t_список (ппгп_id int, num integer, name text) on commit drop;

-- формирование таблицы результата

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''00G'',
''ПАСПОРТ'',
''подпрограммы '' || n || '' «'' || ппгп_наименование || ''» (далее - подпрограмма '' ||9|| '')'' 
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1;

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''99L'',
''_'',''''
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1;

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''01'',
''Наименование подпрограммы ''||n,
''«'' || ппгп_наименование || ''»'' 
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1;

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''02'',
''Наименование государственной программы'',
гп_наименование
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1;

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''03'',
''Ответственный исполнитель  подпрограммы ''||n|| chr(10) ||'' (соисполнитель программы)'',
грбс_наименование  
from (select 1 as a) as ttt
left outer join tППГП as d on 1=1;

insert into t_список
select *
from (
	select distinct
	ппгп_id,
	dense_rank() over(order by грбс_наименование) as num,
	грбс_наименование
	from tОМ 
	where грбс_id not in (select грбс_id from tППГП)
) as d 
order by num;

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''04'',
''Участники подпрограммы'',
case when not exists(select * from t_список) then ''Отсутствуют''
	else string_agg(d.name, '','' || chr(10)) end
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1
left outer join t_список as d on p.ппгп_id = d.ппгп_id
group by nn;

delete from t_список;

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''05'',
''Цель подпрограммы ''||n,
цель_пп_гп_наименование  
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1;

insert into t_список
select *
from (
	select distinct
	ппгп_id,
	num,
	case when num=0 then '''' else cast(num as varchar) || ''. '' end || цельом_наименование	
	from (
		select 
		ппгп_id,
		case when count(цельом_наименование) over(partition by ппгп_id) > 1 then dense_rank() over (partition by ппгп_id order by цельом_наименование) else 0 end as num, 
		цельом_наименование
		from tОМ
	) as f
) as d 
order by num;

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''06'',
''Задачи подпрограммы ''||n,
case when not exists(select * from t_список) then ''Отсутствуют''
	else string_agg(d.name, '','' || chr(10)) || ''.'' end
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1
left outer join t_список as d on p.ппгп_id = d.ппгп_id
group by nn,n;

delete from t_список;

/*
insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''00'',
''Количество показателей подпрограммы ''||n,
cast(rr as varchar)
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1
left outer join (
	select ппгп_id, count(цп_наименование) as rr
	from tЦП
	group by ппгп_id
) as d on p.ппгп_id = d.ппгп_id;
*/

insert into t_список
select *
from (
	select distinct
	cp.ппгп_id,
	num,
	цп_наименование || '' ('' || цп_едизм || ''):'' || chr(10) || значение as наименование
	from tЦП cp 
	left join (
				select 
				ппгп_id,
				цп_id,
				string_agg(цп_значение, chr(10)) as значение
				from tЦП a0
				group by ппгп_id, цп_id
			) zn on cp.ппгп_id=zn.ппгп_id and cp.цп_id=zn.цп_id
) as d 
order by num;

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''07'',
''Целевые показатели (индикаторы) подпрограммы ''||n,
''-'' 
from tППГП
where ппгп_id not in (select ппгп_id from t_список);

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''07''||''_''||num,
case when num=1 then ''Целевые показатели (индикаторы) подпрограммы ''||n else null end,
cast(num as varchar) || ''. '' || name || case when (select max(num) from t_список) = num then ''.'' else ''; '' end
from (select 1 as a) as ttt
inner join tППГП as p on 1=1
inner join t_список as d on p.ппгп_id = d.ппгп_id;

delete from t_список;

insert into ' || v_temp_result || '
select distinct coalesce(nn,''00_'')||''08'',
''Сроки реализации подпрограммы ''||n,
срокреализациис || '' - '' || срокреализациипо
from (select 1 as a) as ttt
inner join tППГП as p on 1=1;

--delete from ' || v_temp_result || ';


insert into ' || v_temp_result || '
--insert into t_список
with ИФ as (
	select 
	ппгп_id	,
	o, c, n
	from (
		select ''1'' as o, ''02'' as c, ''за счет средств республиканского бюджета Карачаево-Черкесской Републики '' as n union
		select ''2'' as o, ''01'' as c, ''за счет средств федерального бюджета (по согласованию) '' as n union
		select ''4'' as o, ''03'' as c, ''за счет средств местных бюджетов (по согласованию) '' as n union
		select ''5'' as o, ''04'' as c, ''за счет внебюджетных средств (по согласованию) '' as n 
	) as d
	inner join (
		select distinct
		p.ппгп_id,
		case when источникфинансирования in (''01'',''02'',''03'') then источникфинансирования else ''04'' end ИФ
		from tОМ as p
		inner join ' || v_table_РО || ' as r on p.ом_id = r.ом_id
	) as d_i on d_i.ИФ = d.c
),
РО as (
	select 
	p.ппгп_id, p.nn,
	case when источникфинансирования in (''01'',''02'',''03'') then источникфинансирования else ''04'' end ИФ,
	case when ' || v_Округление || ' = 0 then round(ба / '||v_ЕдИзм||', 2) else ба end  as ба,
	год
	from tОМ as o
	inner join tППГП as p on o.ппгп_id = p.ппгп_id
	inner join ' || v_table_РО || ' as r on o.ом_id = r.ом_id
),
РО_ит_ИФ as (
	select 
	ппгп_id, nn,
	ИФ,
	' || v_sSum || ' as ба
	from РО
	group by ппгп_id, nn, ИФ
),
РО_ит_Год as (
	select 
	ппгп_id, nn,
	год,
	' || v_sSum || ' as ба
	from РО
	group by ппгп_id, nn, год
	having sum(ба) <> 0 
), 
СтрокиРО as (
	select p.ппгп_id, p.nn||''_0_0000'' as nn, 	
	''Объемы финансового обеспечения государственной программы - '' || '|| v_sSum ||' ||
		case when exists(select * from РО_ит_Год as p0 where p0.ппгп_id = p.ппгп_id) then '', в том числе:'' else '''' end as ss
	from РО as p
	group by p.ппгп_id, p.nn

	union all
	
	Select ппгп_id, p.nn||''_1_'' || y.y as nn, 
	y.y || '' год - '' || ба as ss  
	from t_years as y
	inner join РО_ит_Год as p on y.y = p.год
	union all

	select ИФ.ппгп_id, nn||''_2_''||ИФ.o||''_00'' as nn, 
	ИФ.n || '' - '' || ба || '' , в том числе по годам:'' as ss 
	from ИФ
	inner join РО_ит_ИФ on ИФ.c = РО_ит_ИФ.ИФ and ИФ.ппгп_id = РО_ит_ИФ.ппгп_id

	union all
	Select ИФ0.ппгп_id, nn||''_2_''||ИФ0.o || ''_'' || ИФ0.y as nn, 
	ИФ0.y || '' год - '' || '|| v_sSum ||' as ss  
	from (
			select *
			from ИФ
			cross join t_years
		) as ИФ0
	inner join РО as p on ИФ0.ппгп_id = p.ппгп_id and ИФ0.c = p.ИФ and ИФ0.y = p.год
	group by ИФ0.ппгп_id, ИФ0.o, ИФ0.c, ИФ0.y, nn
)
select replace(r.nn,''__'',''_09_''), 
case when right(r.nn, 7) = ''_0_0000'' then ''Объем финансового обеспечения подпрограммы ''||p.n else '''' end, 
ss
--select ппгп_id, dense_rank()over(partition by ппгп_id order by nn), ss
from (select 1 as a) as ttt
inner join tППГП as p on 1=1
left join СтрокиРО as r on r.ппгп_id = p.ппгп_id;

/*
insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''09'',
''Объем финансового обеспечения подпрограммы ''||n,
string_agg(d.name, '','' || chr(10))
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1
left outer join t_список as d on p.ппгп_id = d.ппгп_id
group by nn,n;
*/

delete from t_список;

insert into t_список
select *
from (
	select distinct
	cp.ппгп_id,
	num,
	цп_наименование || '' ('' || цп_едизм || ''):'' || chr(10) || значение as наименование
	from tЦП cp 
	left join (
				select 
				p.ппгп_id,
				цп_id,
				string_agg(цп_значение, chr(10)) as значение
				from tЦП a0
				inner join tППГП as p on p.год_по = a0.год
				group by p.ппгп_id, цп_id
			) zn on cp.ппгп_id=zn.ппгп_id and cp.цп_id=zn.цп_id
) as d 
order by num;

insert into ' || v_temp_result || '
select coalesce(nn,''00_'')||''10''||''_''||num,
case when num=1 then ''Ожидаемые результаты реализации государственной подпрограммы ''||n else null end,
cast(num as varchar) || ''. '' || name || case when (select max(num) from t_список) = num then ''.'' else ''; '' end
from (select 1 as a) as ttt
left outer join tППГП as p on 1=1
left outer join t_список as d on p.ппгп_id = d.ппгп_id;

delete from ' || v_temp_result || '
where coalesce(Показатель,'''')='''' and coalesce(Значение,'''')=''''	;

';

begin 
	execute (v_sel);
exception when others then
  	get STACKED diagnostics v_msg_text = MESSAGE_TEXT,
                          v_except_detail = PG_EXCEPTION_DETAIL,
                          v_except_hint = PG_EXCEPTION_HINT;
	execute (v_seldel);
	RAISE EXCEPTION 'ошибка формирования результата отчета: %', coalesce(v_msg_text,'')
      USING HINT = v_sel;

end; 

execute (v_seldel);

-- Финальный процесс
if coalesce(v_table_name_result,'') <> '' then

      v_sel := concat('drop table if exists ', v_table_name_result,';create temporary table ', v_table_name_result , ' on commit drop as select  * from ' , v_temp_result , ' order by ord ');
      execute v_sel;

else

      v_sel := concat('select  * from ', v_temp_result, ' order by ord  ');
      return query execute v_sel;

end if;

end;

$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_generate_sl_pivot(v_nmode integer DEFAULT 0)
 RETURNS void
 LANGUAGE plpgsql
AS $functionx$

--do $$

declare
	v_exists_table int;
v_exists_table_new int;
v_sel text;
v_date date;
v_date_t text;
v_newname text;
v_tmp_guid character varying default null;
v_msg_text text;
v_except_detail text;
v_except_hint text;
begin 
	
v_date := current_date;
v_date_t := to_char(v_date, 'dd.mm.yyyy');
v_newname := 'sl_pivot_'||to_char(v_date, 'yyyymmdd');
v_exists_table_new := 0;
SELECT 1 into v_exists_table_new
FROM   pg_catalog.pg_class c
JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE  n.nspname = 'ks_ddlcontrol'
AND    c.relname = v_newname
AND    c.relkind = 'r';
raise notice 'текущий пользователь %', session_user;
if (v_exists_table_new = 1) then 
begin	
	v_sel :='
		drop table ks_ddlcontrol.'||v_newname||';				
	';
execute v_sel;
raise notice '0. удалили %', v_newname;
end;
end if;
v_exists_table := 0;
SELECT 1 into v_exists_table
FROM   pg_catalog.pg_class c
JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE  n.nspname = 'ks_ddlcontrol'
AND    c.relname = 'sl_pivot'
AND    c.relkind = 'r';
if (v_exists_table = 1) then 
begin
	
	v_sel :='
		create table ks_ddlcontrol.'||v_newname||' as 
		select * from ks_ddlcontrol.sl_pivot ;
	';
execute v_sel;
raise notice '1. создали %', v_newname;
v_sel :='
		delete from ks_ddlcontrol.sl_pivot ;
	';
execute v_sel;
raise notice '2. очистили sl_pivot';
-- v_nmode integer
	v_sel :='
 CREATE OR REPLACE FUNCTION ks_ddlcontrol.get_'||v_newname||'(v_nmode integer DEFAULT 0)
 RETURNS SETOF sl_pivot
 LANGUAGE plpgsql
AS $function0$

declare
  func text := ''select * from ks_ddlcontrol.'||v_newname||''';
begin 
    return query execute func;
end;
$function0$
	';
execute v_sel;
raise notice '3. создали функцию %', 'ks_ddlcontrol.get_'||v_newname;
v_sel :='
		insert into dbo.queryitems (objectid, template)	
		select 
		objectid,
		replace(replace(template,''sl_pivot'','''||v_newname||'''),''Актуальный срез данных'',''Срез данных на '||v_date_t||''')
		from dbo.queryitems where id = 26
	';
execute v_sel;
raise notice '4. зарегистрировали запрос "Срез данных на %" в контейнере запросов"', v_date_t;
end;
else
begin
CREATE TABLE ks_ddlcontrol.sl_pivot (
	data integer NULL,
	filename text NULL,
	schet_year integer NULL,
	schet_month integer NULL,
	schet_code_mo text NULL,
	schet_mo_f text NULL,
	schet_mo_s text NULL,
	schet_plat text NULL,
	schet_plat_f text NULL,
	schet_plat_s text NULL,
	schet_nschet text NULL,
	schet_dschet integer NULL,
	schet_disp text NULL,
	schet_disp_f text NULL,
	zap_pacient_id_pac text NULL,
	zap_pacient_fio text NULL,
	zap_pacient_w text NULL,
	zap_pacient_dr integer NULL,
	zap_pacient_fio_p text NULL,
	zap_pacient_w_p text NULL,
	zap_pacient_dr_p integer NULL,
	zap_pacient_novor_w text NULL,
	zap_pacient_novor_dr integer NULL,
	zap_pacient_novor_n integer NULL,
	zap_pacient_vnov_d integer NULL,
	zap_pacient_mr text NULL,
	zap_pacient_okatog text NULL,
	zap_pacient_okatog_f text NULL,
	zap_pacient_okatop text NULL,
	zap_pacient_okatop_f text NULL,
	zap_pacient_snils text NULL,
	zap_pacient_enp text NULL,
	zap_pacient_vpolis text NULL,
	zap_pacient_vpolis_f text NULL,
	zap_pacient_spolis text NULL,
	zap_pacient_npolis text NULL,
	zap_pacient_inv text NULL,
	zap_pacient_mse text NULL,
	zap_z_sl_idcase text NULL,
	zap_z_sl_usl_ok text NULL,
	zap_z_sl_usl_ok_f text NULL,
	zap_z_sl_vidpom text NULL,
	zap_z_sl_vidpom_f text NULL,
	zap_z_sl_for_pom text NULL,
	zap_z_sl_for_pom_f text NULL,
	zap_z_sl_npr_mo text NULL,
	zap_z_sl_npr_mo_s text NULL,
	zap_z_sl_npr_mo_f text NULL,
	zap_z_sl_npr_date integer NULL,
	zap_z_sl_p_disp2 text NULL,
	zap_z_sl_lpu text NULL,
	zap_z_sl_lpu_s text NULL,
	zap_z_sl_lpu_f text NULL,
	zap_z_sl_rslt text NULL,
	zap_z_sl_rslt_f text NULL,
	zap_z_sl_ishod text NULL,
	zap_z_sl_ishod_f text NULL,
	zap_z_sl_rslt_d text NULL,
	zap_z_sl_rslt_d_f text NULL,
	zap_z_sl_idsp text NULL,
	zap_z_sl_idsp_f text NULL,
	zap_z_sl_oplata text NULL,
	zap_z_sl_oplata_f text NULL,
	zap_z_sl_p_otk text NULL,
	zap_z_sl_vbr text NULL,
	zap_z_sl_vnov_m text NULL,
	zap_z_sl_date_z_1 integer NULL,
	zap_z_sl_date_z_2 integer NULL,
	zap_z_sl_kd_z text NULL,
	zap_z_sl_vb_p text NULL,
	zap_z_sl_sl_sl_id text NULL,
	zap_z_sl_sl_vid_hmp text NULL,
	zap_z_sl_sl_vid_hmp_f text NULL,
	zap_z_sl_sl_metod_hmp text NULL,
	zap_z_sl_sl_metod_hmp_f text NULL,
	zap_z_sl_sl_lpu_1 text NULL,
	zap_z_sl_sl_lpu_1_s text NULL,
	zap_z_sl_sl_lpu_1_f text NULL,
	zap_z_sl_sl_podr text NULL,
	zap_z_sl_sl_podr_s text NULL,
	zap_z_sl_sl_podr_f text NULL,
	zap_z_sl_sl_profil text NULL,
	zap_z_sl_sl_profil_f text NULL,
	zap_z_sl_sl_profil_k text NULL,
	zap_z_sl_sl_profil_k_f text NULL,
	zap_z_sl_sl_det text NULL,
	zap_z_sl_sl_det_f text NULL,
	zap_z_sl_sl_p_cel text NULL,
	zap_z_sl_sl_p_cel_f text NULL,
	zap_z_sl_sl_p_per text NULL,
	zap_z_sl_sl_ds0 text NULL,
	zap_z_sl_sl_ds0_f text NULL,
	zap_z_sl_sl_ds1 text NULL,
	zap_z_sl_sl_ds1_f text NULL,
	zap_z_sl_sl_ds1_pr text NULL,
	zap_z_sl_sl_c_zab text NULL,
	zap_z_sl_sl_c_zab_f text NULL,
	zap_z_sl_sl_ds_onk text NULL,
	zap_z_sl_sl_ds_onk_f text NULL,
	zap_z_sl_sl_dn text NULL,
	zap_z_sl_sl_dn_f text NULL,
	zap_z_sl_sl_reab text NULL,
	zap_z_sl_sl_prvs text NULL,
	zap_z_sl_sl_prvs_f text NULL,
	zap_z_sl_sl_iddokt text NULL,
	zap_z_sl_sl_iddokt_fio text NULL,
	zap_z_sl_sl_date_1 integer NULL,
	zap_z_sl_sl_date_2 integer NULL,
	zap_z_sl_sl_tal_p integer NULL,
	zap_z_sl_sl_kd integer NULL,
	zap_z_sl_sl_nhistory text NULL,
	zap_z_sl_sl_ed_col numeric NULL,
	zap_z_sl_sl_tarif numeric NULL,
	zap_z_sl_sl_sum_m numeric NULL,
	zap_z_sl_sl_sum_pr numeric NULL,
	zap_z_sl_sl_mek numeric NULL,
	zap_z_sl_sl_mee numeric NULL,
	zap_z_sl_sl_ekmp numeric NULL,
	zap_z_sl_sl_ds2 text NULL,
	zap_z_sl_sl_ds3 text NULL,
	zap_z_sl_sl_onk_sl_ds1_t text NULL,
	zap_z_sl_sl_onk_sl_stad text NULL,
	zap_z_sl_sl_onk_sl_onk_t text NULL,
	zap_z_sl_sl_onk_sl_onk_n text NULL,
	zap_z_sl_sl_onk_sl_onk_m text NULL,
	zap_z_sl_sl_onk_sl_mtstz text NULL,
	zap_z_sl_sl_onk_sl_sod numeric NULL,
	zap_z_sl_sl_onk_sl_k_fr integer NULL,
	zap_z_sl_sl_onk_sl_wei numeric NULL,
	zap_z_sl_sl_onk_sl_hei integer NULL,
	zap_z_sl_sl_onk_sl_bsa numeric NULL,
	zap_z_sl_sl_ksg_kpg_n_ksg text NULL,
	zap_z_sl_sl_ksg_kpg_n_ksg_f text NULL,
	zap_z_sl_sl_ksg_kpg_n_kpg text NULL,
	zap_z_sl_sl_ksg_kpg_n_kpg_f text NULL,
	zap_z_sl_sl_ksg_kpg_koef_z numeric NULL,
	zap_z_sl_sl_ksg_kpg_koef_up numeric NULL,
	zap_z_sl_sl_ksg_kpg_bztsz numeric NULL,
	zap_z_sl_sl_ksg_kpg_koef_d numeric NULL,
	zap_z_sl_sl_ksg_kpg_koef_u numeric NULL,
	zap_z_sl_sl_ksg_kpg_sl_k_c text NULL,
	zap_z_sl_sl_ksg_kpg_sl_k_f text NULL,
	zap_z_sl_sl_ksg_kpg_it_sl numeric NULL);
end;
end if;
insert into ks_ddlcontrol.sl_pivot  
select 	
cast(to_char(current_date, 'yyyymmdd') as int) as data,
d.filename as FILENAME, -- имя_файла 
d.year as SCHET_YEAR, -- отчетный_год,
d.month as SCHET_MONTH, --отчетный_месяц,
f003.mcod as SCHET_CODE_MO, --мо_код,
f003_d.nam_mop as SCHET_MO_F, --мо_полное_наименование,
f003_d.nam_mok as SCHET_MO_S, --мо_краткое_наименование,
coalesce(f002.smocod, tfoms_16129.kod_tf) as SCHET_PLAT, --смо_код/тфомс_код,
coalesce(f002_d.nam_smop, tfoms_l17512_16088.name_tfp) as SCHET_PLAT_F, --смо_полное_наименование/тфомс_полное,
coalesce(f002_d.nam_smok, tfoms_l17512_16088.name_tfk) as SCHET_PLAT_S, --смо_краткое_наименование/тфомс_краткое,
d.nschet as SCHET_NSCHET, --номер_счета,
dschet_norm as SCHET_DSCHET, --дата_выставления_счета,
v016.iddt as SCHET_DISP, --тип_диспансеризации_код,
v016_vers.dtname as SCHET_DISP_F, --тип_диспансеризации,
pcnt.id_pac as ZAP_PACIENT_ID_PAC, --Код записи (МО)
coalesce(pcnt.fam,'')||' '||coalesce(pcnt.im,'')||' '||coalesce(pcnt.ot,'') as ZAP_PACIENT_FIO, --фамилия_пациента + имя_пациента + отчество_пациента
pcnt.w as ZAP_PACIENT_W, --пол_пациента,
pcnt.dr_norm as ZAP_PACIENT_DR, -- дата_рождения_пациента
coalesce(pcnt.fam_p,'')||' '||coalesce(pcnt.im_p,'')||' '||coalesce(pcnt.ot_p,'') as ZAP_PACIENT_FIO_P, -- фамилия_представителя_пациента
pcnt.w_p as ZAP_PACIENT_W_P, --пол_представителя_пациента,
pcnt.dr_p_norm as ZAP_PACIENT_DR_P, --дата_рождения_представителя,
pcnt_17188.polname as ZAP_PACIENT_NOVOR_W, --пол_новорожденного,
pcnt.novor_d_norm as ZAP_PACIENT_NOVOR_DR, --дата_рождения_новорожденного,
pcnt.novor_nomer :: integer as ZAP_PACIENT_NOVOR_N, --порядковый_номер_новорожденного,
pcnt.vnov_d as ZAP_PACIENT_VNOV_D, -- вес_при_рождении,
pcnt.mr as ZAP_PACIENT_MR, --место_рождения,
pcnt_17183.kod as ZAP_PACIENT_OKATOG, --место_жительства_код,
pcnt_17183_l16613.name1 as ZAP_PACIENT_OKATOG_F, --место_жительства,
pcnt_17184.kod as ZAP_PACIENT_OKATOP, --место_пребывания_код,
pcnt_17184_l16613.name1 as ZAP_PACIENT_OKATOP_F, --место_пребывания,
pcnt.snils as ZAP_PACIENT_SNILS, --снилс,
pcnt.enp as ZAP_PACIENT_ENP, --енп,
pcnt_17143.iddoc as ZAP_PACIENT_VPOLIS, --тип_дпфс_код,
pcnt_17143_l16458.docname as ZAP_PACIENT_VPOLIS_F, --тип_дпфс,
pcnt.spolis as ZAP_PACIENT_SPOLIS, --серия_дпфс,
pcnt.npolis as ZAP_PACIENT_NPOLIS, --номер_дпфс,
pcnt_17150_l18212.name as ZAP_PACIENT_INV, --группа_инвалидности,
pcnt.mse as ZAP_PACIENT_MSE, --направление_на_мсэ,
sl.idcase as ZAP_Z_SL_IDCASE, --номер_записи,
sl_16805.idump as ZAP_Z_SL_USL_OK, --условия_оказания_мп_код,
sl_16805_l16137.umpname as ZAP_Z_SL_USL_OK_F, --условия_оказания_мп,
sl_16806.idvmp as ZAP_Z_SL_VIDPOM, --вид_мп_код,
sl_16806_l16141.vmpname as ZAP_Z_SL_VIDPOM_F, --вид_мп,
sl_16807.idfrmmp as ZAP_Z_SL_FOR_POM, --форма_оказания_мп_код,
sl_16807_l16161.frmmpname as ZAP_Z_SL_FOR_POM_F, --форма_оказания_мп,
sl_16808.mcod as ZAP_Z_SL_NPR_MO,--зсл_мо_код,
sl_16808_l17798_16331.nam_mop as ZAP_Z_SL_NPR_MO_F,--зсл_мо_полное_наименование,
sl_16808_l17798_16331.nam_mok as ZAP_Z_SL_NPR_MO_S,--зсл_мо_краткое_наименование,
sl.npr_date as ZAP_Z_SL_NPR_DATE, --дата_направления,
sl.p_disp2 as ZAP_Z_SL_P_DISP2, --признак_2_этапа_дисп,
sl_16869.mcod as ZAP_Z_SL_LPU, --лпу_мо_код,
sl_16869_l17798_16331.nam_mop as ZAP_Z_SL_LPU_S, --лпу_мо_полное_наименование,
sl_16869_l17798_16331.nam_mok as ZAP_Z_SL_LPU_F,--лпу_мо_краткое_наименование,
sl_16875.idrmp as ZAP_Z_SL_RSLT, --результат_обращения_код,
sl_16875_l16145.rmpname as ZAP_Z_SL_RSLT_F, --результат_обращения,
sl_16876.idiz as ZAP_Z_SL_ISHOD, --исход_заболевания_код,
sl_16876_l16153.izname as ZAP_Z_SL_ISHOD_F, --исход_заболевания,
sl_16878.iddr as ZAP_Z_SL_RSLT_D, --результат_диспансеризации_код,
sl_16878_l16173.drname as ZAP_Z_SL_RSLT_D_F, --результат_диспансеризации,
sl_16881.idsp as ZAP_Z_SL_IDSP, --способ_оплаты_мп_код,
sl_16881_l16149.spname as ZAP_Z_SL_IDSP_F, --способ_оплаты_мп,
sl_16883.kod as ZAP_Z_SL_OPLATA, --тип_оплаты_код,
sl_16883_l18231.name as ZAP_Z_SL_OPLATA_F, --тип_оплаты,
sl.p_otk as ZAP_Z_SL_P_OTK, --признак_отказа,
sl.vbr as ZAP_Z_SL_VBR, --признак_ммб,
zap_18871.vnov_m as ZAP_Z_SL_VNOV_M, --зсл_вес_при_рождении,
sl.date_z_1 as ZAP_Z_SL_DATE_Z_1, --дата_начала_лечения,
sl.date_z_2 as ZAP_Z_SL_DATE_Z_2, --дата_окончания_лечения,
sl.kd_z as ZAP_Z_SL_KD_Z, --продолжительность_госпитализации,
sl.vb_p as ZAP_Z_SL_VB_P, --признак_внутрибольн_перевода,
sl_rs.sl_id as ZAP_Z_SL_SL_SL_ID, --идентификатор_случая,
sl_rs_138119.idhivid as ZAP_Z_SL_SL_VID_HMP, --вид_вмп_код,
sl_rs_138119_l16177.hvidname as ZAP_Z_SL_SL_VID_HMP_F, --вид_вмп,
sl_rs_138120.idhm as ZAP_Z_SL_SL_METOD_HMP, --метод_вмп_код,
sl_rs_138120_l16181.hmname as ZAP_Z_SL_SL_METOD_HMP_F, --метод_вмп,
sl_rs_138031.mpcod as ZAP_Z_SL_SL_LPU_1, --подразделение_мо_код,
sl_rs_138031.nam_mosk as ZAP_Z_SL_SL_LPU_1_S, --подразделение_мо_краткое_наим,
sl_rs_138031.nam_mosp as ZAP_Z_SL_SL_LPU_1_F, --подразделение_мо_полное_наим,
sl_rs_138043.mosocod as ZAP_Z_SL_SL_PODR, --отделение_подр_мо_код,
sl_rs_138043.nam_mosok as ZAP_Z_SL_SL_PODR_S, --отделение_подр_мо_краткое_наим,
sl_rs_138043.nam_mosop as ZAP_Z_SL_SL_PODR_F, --отделение_подр_мо_полное_наим,
sl_rs_138121.idpr as ZAP_Z_SL_SL_PROFIL, --профиль_мп_код,
sl_rs_138121_l16107.prname as ZAP_Z_SL_SL_PROFIL_F, --профиль_мп,
sl_rs_138122.idk_pr as ZAP_Z_SL_SL_PROFIL_K, --профиль_койки_код,
sl_rs_138122_l16189.k_prname as ZAP_Z_SL_SL_PROFIL_K_F, --профиль_койки,
sl_rs_138123.kod as ZAP_Z_SL_SL_DET, --признак_детского_профиля_код,
sl_rs_138123_l18237.name as ZAP_Z_SL_SL_DET_F, --признак_детского_профиля,
sl_rs_138124.idpc as ZAP_Z_SL_SL_P_CEL, --цель_посещения_код,
sl_rs_138124_l16209.n_pc as ZAP_Z_SL_SL_P_CEL_F, --цель_посещения,
sl_rs_138130_l18244.name as ZAP_Z_SL_SL_P_PER, --признак_поступления_перевода,
sl_rs_138134.mkb_code as ZAP_Z_SL_SL_DS0, --диагноз_первичный_код,
sl_rs_138134_l19902.mkb_name as ZAP_Z_SL_SL_DS0_F, --диагноз_первичный,
sl_rs_138135.mkb_code as ZAP_Z_SL_SL_DS1, --диагноз_основной_код,
sl_rs_138135_l19902.mkb_name as ZAP_Z_SL_SL_DS1_F, --диагноз_основной,
sl_rs.ds1_pr as ZAP_Z_SL_SL_DS1_PR, --установлен_впервые,
sl_rs_138143.idcz as ZAP_Z_SL_SL_C_ZAB, --характер_осн_заболевания_код,
sl_rs_138143_l16217.n_cz as ZAP_Z_SL_SL_C_ZAB_F, --характер_осн_заболевания,
sl_rs_138139.kod as ZAP_Z_SL_SL_DS_ONK, --признак_подозрения на злокачественное новообразование _код,
sl_rs_138139_l18250.name as ZAP_Z_SL_SL_DS_ONK_F, --признак_подозрения на злокачественное новообразование _зн,
coalesce(sl_rs_138141.kod, sl_rs_138142.kod) as ZAP_Z_SL_SL_DN, --диспансерное_наблюдение_x_файл_код / диспансерное_наблюдение_код
coalesce(sl_rs_138141_l18466.name, sl_rs_138142_l18466.name) as ZAP_Z_SL_SL_DN, --диспансерное_наблюдение_x_файл / диспансерное_наблюдение
sl_rs.reab as ZAP_Z_SL_SL_REAB, --признак_реабилитации,
sl_rs_138177.idspec as ZAP_Z_SL_SL_PRVS, --специальность_врача_код,
sl_rs_138177_l16193.specname as ZAP_Z_SL_SL_PRVS_F, --специальность_врача,
sl_rs_138179.iddokt as ZAP_Z_SL_SL_IDDOKT, --лечащий_врач_код,
coalesce(sl_rs_138179_l18280.fam,'')||' '||coalesce(sl_rs_138179_l18280.im,'')||' '||coalesce(sl_rs_138179_l18280.ot,'') as ZAP_Z_SL_SL_IDDOKT_FIO, -- лечащий_врач_фамилия + лечащий_врач_имя + лечащий_врач_отчество,
sl_rs.date_1 as ZAP_Z_SL_SL_DATE_1, --рс_дата_начала_лечения,
sl_rs.date_2 as ZAP_Z_SL_SL_DATE_2, --рс_дата_окончания_лечения,
sl_rs.tal_p as ZAP_Z_SL_SL_TAL_P, --дата_планируемой_госпитализации,
sl_rs.kd as ZAP_Z_SL_SL_KD, -- Запрос Данные о СЛ. Продолжительность госпитализации,
sl_rs.nhistory as ZAP_Z_SL_SL_NHISTORY, -- Запрос Данные о СЛ. Номер истории/талона/карты вызова,
sl_rs.ed_col as ZAP_Z_SL_SL_ED_COL, --количество_единиц_оплаты_мп,
sl_rs.tarif as ZAP_Z_SL_SL_TARIF, --тариф
sl_rs.sum_m as ZAP_Z_SL_SL_SUM_M, --стоимость_выставленная,
case when exp001.реестр_законченных_случаев_id is null then sl_rs.sum_m else 0 end as ZAP_Z_SL_SL_SUM_PR,
case when exp001.реестр_законченных_случаев_id is not null then sl_rs.sum_m else 0 end as ZAP_Z_SL_SL_MEK,
0.0 as ZAP_Z_SL_SL_MEE,
0.0 as ZAP_Z_SL_SL_EKMP,
mkb.наименование as ZAP_Z_SL_SL_DS2,
mkb_o.наименование as ZAP_Z_SL_SL_DS3,
sl_rs_138230_l16297.reas_name as ZAP_Z_SL_SL_ONK_SL_DS1_T, --повод_обращения,
sl_rs_138226_l16233.kod_st as ZAP_Z_SL_SL_ONK_SL_STAD, --стадия_заболевания,
sl_rs_138227_l16237.kod_t as ZAP_Z_SL_SL_ONK_SL_ONK_T, --tumor,
sl_rs_138228_l16241.kod_n as ZAP_Z_SL_SL_ONK_SL_ONK_N, --nodus,
sl_rs_138229_l16245.kod_m as ZAP_Z_SL_SL_ONK_SL_ONK_M, --metastasis,
sl_rs.onk_sl_mtstz as ZAP_Z_SL_SL_ONK_SL_MTSTZ, --признак_метастазов,
sl_rs.onk_sl_sod as ZAP_Z_SL_SL_ONK_SL_SOD, --доза,
sl_rs.onk_sl_k_fr as ZAP_Z_SL_SL_ONK_SL_K_FR, --количество_фракций,
sl_rs.onk_sl_wei as ZAP_Z_SL_SL_ONK_SL_WEI, --масса,
sl_rs.onk_sl_hei as ZAP_Z_SL_SL_ONK_SL_HEI, --рост,
sl_rs.onk_sl_bsa as ZAP_Z_SL_SL_ONK_SL_BSA, --площадь,
sl_rs_138255.k_ksg as ZAP_Z_SL_SL_KSG_KPG_N_KSG, --ксг_номер,
sl_rs_138255_l16201.n_ksg as ZAP_Z_SL_SL_KSG_KPG_N_KSG_F, --ксг_наименование,
sl_rs_138258.k_kpg as ZAP_Z_SL_SL_KSG_KPG_N_KPG, --кпг_номер,
sl_rs_138258_l16213.n_kpg as ZAP_Z_SL_SL_KSG_KPG_N_KPG_F, --кпг_наименование	
sl_rs.ksg_kpg_koef_z as ZAP_Z_SL_SL_KSG_KPG_KOEF_Z, --коэффициент_затратоемкости,
sl_rs.ksg_kpg_koef_up as ZAP_Z_SL_SL_KSG_KPG_KOEF_UP, --управленческий_коэффициент,
sl_rs.ksg_kpg_bztsz as ZAP_Z_SL_SL_KSG_KPG_BZTSZ, --базовая_ставка,
sl_rs.ksg_kpg_koef_d as ZAP_Z_SL_SL_KSG_KPG_KOEF_D, --коэффициент_дифференциации,
sl_rs.ksg_kpg_koef_u as ZAP_Z_SL_SL_KSG_KPG_KOEF_UZAP_Z_SL_SL_KSG_KPG_KOEF_U, --коэффициент_оказания_мп,
sl_rs_138265.kod as ZAP_Z_SL_SL_KSG_KPG_SL_K_C, --признак_использования_кслп_код,
sl_rs_138265_l18269.name as ZAP_Z_SL_SL_KSG_KPG_SL_K_F, --признак_использования_кслп
sl_rs.ksg_kpg_it_sl as ZAP_Z_SL_SL_KSG_KPG_IT_SL --примененный_кслп

from ks_ddlcontrol.zl_list as d -- мид «сведения об оказанной мп» (3107)

inner join ks_ddlcontrol.cl_958_3060 t_17517 on d.status = t_17517.id and t_17517.at_3067 > 0  -- Статус. Приоритет > 0

inner join ks_ddlcontrol.f003_r as f003 on f003.id = d.code_mo_f003 -- спр. Мо
inner join ks_ddlcontrol.f003_r_vers as f003_v on f003.id = f003_v.id_up 
	and f003_v.date = (select max(date) from ks_ddlcontrol.f003_r_vers as f003_v0 
			where f003_v0.id_up=f003_v.id_up and f003_v0.date <= d.dschet_norm ) 
inner join ks_ddlcontrol.f003_d as f003_d on f003_v.f003_d = f003_d.id

left join ks_ddlcontrol.f002_r as f002 on f002.id = d.plat_f002 -- спр. Смо
left join ks_ddlcontrol.f002_r_vers as f002_v on f002.id = f002_v.id_up 
	and f002_v.date = (select max(date) from ks_ddlcontrol.f002_r_vers as f002_v0 
			where f002_v0.id_up=f002_v.id_up and f002_v0.date <= d.dschet_norm ) 
left join ks_ddlcontrol.f002_d as f002_d on f002_v.f002_d = f002_d.id

left join ks_ddlcontrol.v016 on v016.id = d.disp_v016 -- тип диспансеризации
left join ks_ddlcontrol.v016_vers on v016_vers.id_up = v016.id
	and v016_vers.datebeg = (select max(datebeg) from ks_ddlcontrol.v016_vers as v016_vers0 
			where v016_vers0.id_up=v016_vers.id_up and v016_vers0.datebeg <= d.dschet_norm )
			
inner join ks_ddlcontrol.zl_list_zap as zap on zap.id_up = d.id -- мид «сведения об оказанной мп» \ тч записи
inner join ks_ddlcontrol.z_sl as sl on sl.id = zap.z_sl_spr -- спр. Реестр законченных случаев (3093)
inner join ks_ddlcontrol.tbl_pacient as pcnt on pcnt.id = zap.pacient_spr -- спр. Сведения о пациенте (3106)

left outer join ks_ddlcontrol.f008 pcnt_17143 on pcnt.vpolis_f008 = pcnt_17143.id
left outer join ks_ddlcontrol.f008_vers pcnt_17143_l16458 on pcnt_17143.id = pcnt_17143_l16458.id_up and coalesce(pcnt_17143_l16458.datebeg,0) = (select  	max(coalesce(pcnt_17143_l16458_v.datebeg,0))  from  	ks_ddlcontrol.f008_vers pcnt_17143_l16458_v where  	pcnt_17143_l16458_v.id_up = pcnt_17143_l16458.id_up and coalesce(pcnt_17143_l16458_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.inv pcnt_17150 on pcnt.inv_spr = pcnt_17150.id
left outer join ks_ddlcontrol.inv_vers pcnt_17150_l18212 on pcnt_17150.id = pcnt_17150_l18212.id_up and coalesce(pcnt_17150_l18212.datebeg,0) = (select  	max(coalesce(pcnt_17150_l18212_v.datebeg,0))  from  	ks_ddlcontrol.inv_vers pcnt_17150_l18212_v where  	pcnt_17150_l18212_v.id_up = pcnt_17150_l18212.id_up and coalesce(pcnt_17150_l18212_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.o002 pcnt_17183 on pcnt.okatog_o002 = pcnt_17183.id
left outer join ks_ddlcontrol.o002_vers pcnt_17183_l16613 on pcnt_17183.id = pcnt_17183_l16613.id_up and coalesce(pcnt_17183_l16613.datevved,0) = (select  	max(coalesce(pcnt_17183_l16613_v.datevved,0))  from  	ks_ddlcontrol.o002_vers pcnt_17183_l16613_v where  	pcnt_17183_l16613_v.id_up = pcnt_17183_l16613.id_up and coalesce(pcnt_17183_l16613_v.datevved,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.o002 pcnt_17184 on pcnt.okatop_o002 = pcnt_17184.id
left outer join ks_ddlcontrol.o002_vers pcnt_17184_l16613 on pcnt_17184.id = pcnt_17184_l16613.id_up and coalesce(pcnt_17184_l16613.datevved,0) = (select  	max(coalesce(pcnt_17184_l16613_v.datevved,0))  from  	ks_ddlcontrol.o002_vers pcnt_17184_l16613_v where  	pcnt_17184_l16613_v.id_up = pcnt_17184_l16613.id_up and coalesce(pcnt_17184_l16613_v.datevved,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v005 pcnt_17188 on pcnt.novor_w_v005 = pcnt_17188.id

left outer join ks_ddlcontrol.v006 sl_16805 on sl.usl_ok_v006 = sl_16805.id
left outer join ks_ddlcontrol.v006_vers sl_16805_l16137 on sl_16805.id = sl_16805_l16137.id_up and coalesce(sl_16805_l16137.datebeg,0) = (select  	max(coalesce(sl_16805_l16137_v.datebeg,0))  from  	ks_ddlcontrol.v006_vers sl_16805_l16137_v where  	sl_16805_l16137_v.id_up = sl_16805_l16137.id_up and coalesce(sl_16805_l16137_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v008 sl_16806 on sl.vidpom_v008 = sl_16806.id
left outer join ks_ddlcontrol.v008_vers sl_16806_l16141 on sl_16806.id = sl_16806_l16141.id_up and coalesce(sl_16806_l16141.datebeg,0) = (select  	max(coalesce(sl_16806_l16141_v.datebeg,0))  from  	ks_ddlcontrol.v008_vers sl_16806_l16141_v where  	sl_16806_l16141_v.id_up = sl_16806_l16141.id_up and coalesce(sl_16806_l16141_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v014 sl_16807 on sl.for_pom_v014 = sl_16807.id
left outer join ks_ddlcontrol.v014_vers sl_16807_l16161 on sl_16807.id = sl_16807_l16161.id_up and coalesce(sl_16807_l16161.datebeg,0) = (select  	max(coalesce(sl_16807_l16161_v.datebeg,0))  from  	ks_ddlcontrol.v014_vers sl_16807_l16161_v where  	sl_16807_l16161_v.id_up = sl_16807_l16161.id_up and coalesce(sl_16807_l16161_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.f003_r sl_16808 on sl.npr_mo_f003 = sl_16808.id
left outer join ks_ddlcontrol.f003_r_vers sl_16808_l17798 on sl_16808.id = sl_16808_l17798.id_up and coalesce(sl_16808_l17798.date,0) = (select  	max(coalesce(sl_16808_l17798_v.date,0))  from  	ks_ddlcontrol.f003_r_vers sl_16808_l17798_v where  	sl_16808_l17798_v.id_up = sl_16808_l17798.id_up and coalesce(sl_16808_l17798_v.date,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.f003_d sl_16808_l17798_16331 on sl_16808_l17798.f003_d = sl_16808_l17798_16331.id
left outer join ks_ddlcontrol.f003_r sl_16869 on sl.lpu_f003 = sl_16869.id
left outer join ks_ddlcontrol.f003_r_vers sl_16869_l17798 on sl_16869.id = sl_16869_l17798.id_up and coalesce(sl_16869_l17798.date,0) = (select  	max(coalesce(sl_16869_l17798_v.date,0))  from  	ks_ddlcontrol.f003_r_vers sl_16869_l17798_v where  	sl_16869_l17798_v.id_up = sl_16869_l17798.id_up and coalesce(sl_16869_l17798_v.date,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.f003_d sl_16869_l17798_16331 on sl_16869_l17798.f003_d = sl_16869_l17798_16331.id
left outer join ks_ddlcontrol.v009 sl_16875 on sl.rslt_v009 = sl_16875.id
left outer join ks_ddlcontrol.v009_vers sl_16875_l16145 on sl_16875.id = sl_16875_l16145.id_up and coalesce(sl_16875_l16145.datebeg,0) = (select  	max(coalesce(sl_16875_l16145_v.datebeg,0))  from  	ks_ddlcontrol.v009_vers sl_16875_l16145_v where  	sl_16875_l16145_v.id_up = sl_16875_l16145.id_up and coalesce(sl_16875_l16145_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v012 sl_16876 on sl.ishod_v012 = sl_16876.id
left outer join ks_ddlcontrol.v012_vers sl_16876_l16153 on sl_16876.id = sl_16876_l16153.id_up and coalesce(sl_16876_l16153.datebeg,0) = (select  	max(coalesce(sl_16876_l16153_v.datebeg,0))  from  	ks_ddlcontrol.v012_vers sl_16876_l16153_v where  	sl_16876_l16153_v.id_up = sl_16876_l16153.id_up and coalesce(sl_16876_l16153_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v017 sl_16878 on sl.rslt_d_v017 = sl_16878.id
left outer join ks_ddlcontrol.v017_vers sl_16878_l16173 on sl_16878.id = sl_16878_l16173.id_up and coalesce(sl_16878_l16173.datebeg,0) = (select  	max(coalesce(sl_16878_l16173_v.datebeg,0))  from  	ks_ddlcontrol.v017_vers sl_16878_l16173_v where  	sl_16878_l16173_v.id_up = sl_16878_l16173.id_up and coalesce(sl_16878_l16173_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v010 sl_16881 on sl.idsp_v010 = sl_16881.id
left outer join ks_ddlcontrol.v010_vers sl_16881_l16149 on sl_16881.id = sl_16881_l16149.id_up and coalesce(sl_16881_l16149.datebeg,0) = (select  	max(coalesce(sl_16881_l16149_v.datebeg,0))  from  	ks_ddlcontrol.v010_vers sl_16881_l16149_v where  	sl_16881_l16149_v.id_up = sl_16881_l16149.id_up and coalesce(sl_16881_l16149_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.oplata sl_16883 on sl.oplata_rs005 = sl_16883.id
left outer join ks_ddlcontrol.oplata_vers sl_16883_l18231 on sl_16883.id = sl_16883_l18231.id_up and coalesce(sl_16883_l18231.datebeg,0) = (select  	max(coalesce(sl_16883_l18231_v.datebeg,0))  from  	ks_ddlcontrol.oplata_vers sl_16883_l18231_v where  	sl_16883_l18231_v.id_up = sl_16883_l18231.id_up and coalesce(sl_16883_l18231_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.z_sl_sl sl_l18434 on sl.id = sl_l18434.id_up

left outer join ks_ddlcontrol.sl sl_rs on sl_l18434.sl_spr = sl_rs.id -- 7.	Данные о СЛ
left outer join ks_ddlcontrol.v018 sl_rs_138119 on sl_rs.vid_hmp_v018 = sl_rs_138119.id
left outer join ks_ddlcontrol.v018_vers sl_rs_138119_l16177 on sl_rs_138119.id = sl_rs_138119_l16177.id_up and coalesce(sl_rs_138119_l16177.datebeg,0) = (select  	max(coalesce(sl_rs_138119_l16177_v.datebeg,0))  from  	ks_ddlcontrol.v018_vers sl_rs_138119_l16177_v where  	sl_rs_138119_l16177_v.id_up = sl_rs_138119_l16177.id_up and coalesce(sl_rs_138119_l16177_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v019 sl_rs_138120 on sl_rs.metod_hmp_v019 = sl_rs_138120.id
left outer join ks_ddlcontrol.v019_vers sl_rs_138120_l16181 on sl_rs_138120.id = sl_rs_138120_l16181.id_up and coalesce(sl_rs_138120_l16181.datebeg,0) = (select  	max(coalesce(sl_rs_138120_l16181_v.datebeg,0))  from  	ks_ddlcontrol.v019_vers sl_rs_138120_l16181_v where  	sl_rs_138120_l16181_v.id_up = sl_rs_138120_l16181.id_up and coalesce(sl_rs_138120_l16181_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.f003_sp_podr sl_rs_138031 on sl_rs.lpu_1_f003_sp = sl_rs_138031.id
left outer join ks_ddlcontrol.rs022 sl_rs_138043 on sl_rs.podr_rs022 = sl_rs_138043.id
left outer join ks_ddlcontrol.v002 sl_rs_138121 on sl_rs.profil_v002 = sl_rs_138121.id
left outer join ks_ddlcontrol.v002_vers sl_rs_138121_l16107 on sl_rs_138121.id = sl_rs_138121_l16107.id_up and coalesce(sl_rs_138121_l16107.datebeg,0) = (select  	max(coalesce(sl_rs_138121_l16107_v.datebeg,0))  from  	ks_ddlcontrol.v002_vers sl_rs_138121_l16107_v where  	sl_rs_138121_l16107_v.id_up = sl_rs_138121_l16107.id_up and coalesce(sl_rs_138121_l16107_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v020 sl_rs_138122 on sl_rs.profil_k_v020 = sl_rs_138122.id
left outer join ks_ddlcontrol.v020_vers sl_rs_138122_l16189 on sl_rs_138122.id = sl_rs_138122_l16189.id_up and coalesce(sl_rs_138122_l16189.datebeg,0) = (select  	max(coalesce(sl_rs_138122_l16189_v.datebeg,0))  from  	ks_ddlcontrol.v020_vers sl_rs_138122_l16189_v where  	sl_rs_138122_l16189_v.id_up = sl_rs_138122_l16189.id_up and coalesce(sl_rs_138122_l16189_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.det sl_rs_138123 on sl_rs.det_rs006 = sl_rs_138123.id
left outer join ks_ddlcontrol.det_vers sl_rs_138123_l18237 on sl_rs_138123.id = sl_rs_138123_l18237.id_up and coalesce(sl_rs_138123_l18237.datebeg,0) = (select  	max(coalesce(sl_rs_138123_l18237_v.datebeg,0))  from  	ks_ddlcontrol.det_vers sl_rs_138123_l18237_v where  	sl_rs_138123_l18237_v.id_up = sl_rs_138123_l18237.id_up and coalesce(sl_rs_138123_l18237_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v025 sl_rs_138124 on sl_rs.p_cel_v025 = sl_rs_138124.id
left outer join ks_ddlcontrol.v025_vers sl_rs_138124_l16209 on sl_rs_138124.id = sl_rs_138124_l16209.id_up and coalesce(sl_rs_138124_l16209.datebeg,0) = (select  	max(coalesce(sl_rs_138124_l16209_v.datebeg,0))  from  	ks_ddlcontrol.v025_vers sl_rs_138124_l16209_v where  	sl_rs_138124_l16209_v.id_up = sl_rs_138124_l16209.id_up and coalesce(sl_rs_138124_l16209_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.p_per sl_rs_138130 on sl_rs.p_per_rs007 = sl_rs_138130.id
left outer join ks_ddlcontrol.p_per_vers sl_rs_138130_l18244 on sl_rs_138130.id = sl_rs_138130_l18244.id_up and coalesce(sl_rs_138130_l18244.datebeg,0) = (select  	max(coalesce(sl_rs_138130_l18244_v.datebeg,0))  from  	ks_ddlcontrol.p_per_vers sl_rs_138130_l18244_v where  	sl_rs_138130_l18244_v.id_up = sl_rs_138130_l18244.id_up and coalesce(sl_rs_138130_l18244_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.m001 sl_rs_138134 on sl_rs.ds0_m001 = sl_rs_138134.id
left outer join ks_ddlcontrol.m001_vers sl_rs_138134_l19902 on sl_rs_138134.id = sl_rs_138134_l19902.id_up and coalesce(sl_rs_138134_l19902.datebeg,0) = (select  	max(coalesce(sl_rs_138134_l19902_v.datebeg,0))  from  	ks_ddlcontrol.m001_vers sl_rs_138134_l19902_v where  	sl_rs_138134_l19902_v.id_up = sl_rs_138134_l19902.id_up and coalesce(sl_rs_138134_l19902_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.m001 sl_rs_138135 on sl_rs.ds1_m001 = sl_rs_138135.id
left outer join ks_ddlcontrol.m001_vers sl_rs_138135_l19902 on sl_rs_138135.id = sl_rs_138135_l19902.id_up and coalesce(sl_rs_138135_l19902.datebeg,0) = (select  	max(coalesce(sl_rs_138135_l19902_v.datebeg,0))  from  	ks_ddlcontrol.m001_vers sl_rs_138135_l19902_v where  	sl_rs_138135_l19902_v.id_up = sl_rs_138135_l19902.id_up and coalesce(sl_rs_138135_l19902_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v027 sl_rs_138143 on sl_rs.c_zab_v027 = sl_rs_138143.id
left outer join ks_ddlcontrol.v027_vers sl_rs_138143_l16217 on sl_rs_138143.id = sl_rs_138143_l16217.id_up and coalesce(sl_rs_138143_l16217.datebeg,0) = (select  	max(coalesce(sl_rs_138143_l16217_v.datebeg,0))  from  	ks_ddlcontrol.v027_vers sl_rs_138143_l16217_v where  	sl_rs_138143_l16217_v.id_up = sl_rs_138143_l16217.id_up and coalesce(sl_rs_138143_l16217_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.ds_onk sl_rs_138139 on sl_rs.ds_onk_rs008 = sl_rs_138139.id
left outer join ks_ddlcontrol.ds_onk_vers sl_rs_138139_l18250 on sl_rs_138139.id = sl_rs_138139_l18250.id_up and coalesce(sl_rs_138139_l18250.datebeg,0) = (select  	max(coalesce(sl_rs_138139_l18250_v.datebeg,0))  from  	ks_ddlcontrol.ds_onk_vers sl_rs_138139_l18250_v where  	sl_rs_138139_l18250_v.id_up = sl_rs_138139_l18250.id_up and coalesce(sl_rs_138139_l18250_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.rs015 sl_rs_138141 on sl_rs.pr_d_n_rs015 = sl_rs_138141.id
left outer join ks_ddlcontrol.rs015_vers sl_rs_138141_l18466 on sl_rs_138141.id = sl_rs_138141_l18466.id_up and coalesce(sl_rs_138141_l18466.datebeg,0) = (select  	max(coalesce(sl_rs_138141_l18466_v.datebeg,0))  from  	ks_ddlcontrol.rs015_vers sl_rs_138141_l18466_v where  	sl_rs_138141_l18466_v.id_up = sl_rs_138141_l18466.id_up and coalesce(sl_rs_138141_l18466_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.rs015 sl_rs_138142 on sl_rs.dn_rs015 = sl_rs_138142.id
left outer join ks_ddlcontrol.rs015_vers sl_rs_138142_l18466 on sl_rs_138142.id = sl_rs_138142_l18466.id_up and coalesce(sl_rs_138142_l18466.datebeg,0) = (select  	max(coalesce(sl_rs_138142_l18466_v.datebeg,0))  from  	ks_ddlcontrol.rs015_vers sl_rs_138142_l18466_v where  	sl_rs_138142_l18466_v.id_up = sl_rs_138142_l18466.id_up and coalesce(sl_rs_138142_l18466_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v021 sl_rs_138177 on sl_rs.prvs_v021 = sl_rs_138177.id
left outer join ks_ddlcontrol.v021_vers sl_rs_138177_l16193 on sl_rs_138177.id = sl_rs_138177_l16193.id_up and coalesce(sl_rs_138177_l16193.datebeg,0) = (select  	max(coalesce(sl_rs_138177_l16193_v.datebeg,0))  from  	ks_ddlcontrol.v021_vers sl_rs_138177_l16193_v where  	sl_rs_138177_l16193_v.id_up = sl_rs_138177_l16193.id_up and coalesce(sl_rs_138177_l16193_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.rs013 sl_rs_138179 on sl_rs.iddokt_rs013 = sl_rs_138179.id
left outer join ks_ddlcontrol.rs013_vers sl_rs_138179_l18280 on sl_rs_138179.id = sl_rs_138179_l18280.id_up and coalesce(sl_rs_138179_l18280.datebeg,0) = (select  	max(coalesce(sl_rs_138179_l18280_v.datebeg,0))  from  	ks_ddlcontrol.rs013_vers sl_rs_138179_l18280_v where  	sl_rs_138179_l18280_v.id_up = sl_rs_138179_l18280.id_up and coalesce(sl_rs_138179_l18280_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.n018 sl_rs_138230 on sl_rs.onk_sl_ds1_t_n018 = sl_rs_138230.id
left outer join ks_ddlcontrol.n018_vers sl_rs_138230_l16297 on sl_rs_138230.id = sl_rs_138230_l16297.id_up and coalesce(sl_rs_138230_l16297.datebeg,0) = (select  	max(coalesce(sl_rs_138230_l16297_v.datebeg,0))  from  	ks_ddlcontrol.n018_vers sl_rs_138230_l16297_v where  	sl_rs_138230_l16297_v.id_up = sl_rs_138230_l16297.id_up and coalesce(sl_rs_138230_l16297_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.n002 sl_rs_138226 on sl_rs.onk_sl_stad_n002 = sl_rs_138226.id
left outer join ks_ddlcontrol.n002_vers sl_rs_138226_l16233 on sl_rs_138226.id = sl_rs_138226_l16233.id_up and coalesce(sl_rs_138226_l16233.datebeg,0) = (select  	max(coalesce(sl_rs_138226_l16233_v.datebeg,0))  from  	ks_ddlcontrol.n002_vers sl_rs_138226_l16233_v where  	sl_rs_138226_l16233_v.id_up = sl_rs_138226_l16233.id_up and coalesce(sl_rs_138226_l16233_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.n003 sl_rs_138227 on sl_rs.onk_sl_onk_t_n003 = sl_rs_138227.id
left outer join ks_ddlcontrol.n003_vers sl_rs_138227_l16237 on sl_rs_138227.id = sl_rs_138227_l16237.id_up and coalesce(sl_rs_138227_l16237.datebeg,0) = (select  	max(coalesce(sl_rs_138227_l16237_v.datebeg,0))  from  	ks_ddlcontrol.n003_vers sl_rs_138227_l16237_v where  	sl_rs_138227_l16237_v.id_up = sl_rs_138227_l16237.id_up and coalesce(sl_rs_138227_l16237_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.n004 sl_rs_138228 on sl_rs.onk_sl_onk_n_n004 = sl_rs_138228.id
left outer join ks_ddlcontrol.n004_vers sl_rs_138228_l16241 on sl_rs_138228.id = sl_rs_138228_l16241.id_up and coalesce(sl_rs_138228_l16241.datebeg,0) = (select  	max(coalesce(sl_rs_138228_l16241_v.datebeg,0))  from  	ks_ddlcontrol.n004_vers sl_rs_138228_l16241_v where  	sl_rs_138228_l16241_v.id_up = sl_rs_138228_l16241.id_up and coalesce(sl_rs_138228_l16241_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.n005 sl_rs_138229 on sl_rs.onk_sl_onk_m_n005 = sl_rs_138229.id
left outer join ks_ddlcontrol.n005_vers sl_rs_138229_l16245 on sl_rs_138229.id = sl_rs_138229_l16245.id_up and coalesce(sl_rs_138229_l16245.datebeg,0) = (select  	max(coalesce(sl_rs_138229_l16245_v.datebeg,0))  from  	ks_ddlcontrol.n005_vers sl_rs_138229_l16245_v where  	sl_rs_138229_l16245_v.id_up = sl_rs_138229_l16245.id_up and coalesce(sl_rs_138229_l16245_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v023 sl_rs_138255 on sl_rs.ksg_kpg_n_ksg_v023 = sl_rs_138255.id
left outer join ks_ddlcontrol.v023_vers sl_rs_138255_l16201 on sl_rs_138255.id = sl_rs_138255_l16201.id_up and coalesce(sl_rs_138255_l16201.datebeg,0) = (select  	max(coalesce(sl_rs_138255_l16201_v.datebeg,0))  from  	ks_ddlcontrol.v023_vers sl_rs_138255_l16201_v where  	sl_rs_138255_l16201_v.id_up = sl_rs_138255_l16201.id_up and coalesce(sl_rs_138255_l16201_v.datebeg,0) <= coalesce(sl.date_z_1,0) )
left outer join ks_ddlcontrol.v026 sl_rs_138258 ON sl_rs.ksg_kpg_n_kpg_v026 = sl_rs_138258.id
left outer join ks_ddlcontrol.v026_vers sl_rs_138258_l16213 on sl_rs_138255.id = sl_rs_138258_l16213.id_up and coalesce(sl_rs_138255_l16201.datebeg,0) = (select  	max(coalesce(sl_rs_138258_l16213_v.datebeg,0))  from  	ks_ddlcontrol.v026_vers sl_rs_138258_l16213_v where  	sl_rs_138258_l16213_v.id_up = sl_rs_138258_l16213.id_up and coalesce(sl_rs_138258_l16213_v.datebeg,0) <= coalesce(sl.date_z_1,0) )

left outer join ks_ddlcontrol.sl_k sl_rs_138265 on sl_rs.ksg_kpg_sl_k_rs011 = sl_rs_138265.id
left outer join ks_ddlcontrol.sl_k_vers sl_rs_138265_l18269 on sl_rs_138265.id = sl_rs_138265_l18269.id_up and coalesce(sl_rs_138265_l18269.datebeg,0) = (select  	max(coalesce(sl_rs_138265_l18269_v.datebeg,0))  from  	ks_ddlcontrol.sl_k_vers sl_rs_138265_l18269_v where  	sl_rs_138265_l18269_v.id_up = sl_rs_138265_l18269.id_up and coalesce(sl_rs_138265_l18269_v.datebeg,0) <= coalesce(sl.date_z_1,0) )

left outer join ( -- 5.	ЗСЛ вес при рождении
		select 
		id_up,
		string_agg(cast(vnov_m as text), ';') as vnov_m
		from ks_ddlcontrol.z_sl_vnov_m  
		group by id_up
	) zap_18871 on zap.id = zap_18871.id_up
	
left outer join  ks_ddlcontrol.f001_r tfoms on 1=1
left outer join ks_ddlcontrol.f010 tfoms_16129 on tfoms.tf_kod = tfoms_16129.id and tfoms_16129.kod_tf = '04'
left outer join ks_ddlcontrol.f001_r_vers tfoms_l17512 on tfoms.id = tfoms_l17512.id_up and coalesce(tfoms_l17512.date,0) = (select max(coalesce(tfoms_l17512_v.date,0))  from ks_ddlcontrol.f001_r_vers tfoms_l17512_v where tfoms_l17512_v.id_up = tfoms_l17512.id_up and coalesce(tfoms_l17512_v.date,0) <= coalesce(d.dschet_norm,0) ) 
left outer join ks_ddlcontrol.f001_d tfoms_l17512_16088 on tfoms_l17512.f001_d = tfoms_l17512_16088.id		

left outer join ( -- 12.	Санкции МЭК
		select 
		t.zl_list as реестр_счетов_id,
		t_140210.at_3067 as статус_приоритет,
		t_l22463.iserror as законченные_случаи_нарушение,
		t_l22463.zsl as реестр_законченных_случаев_id
		from ks_ddlcontrol.exp001 t
		INNER JOIN ks_ddlcontrol.cl_958_3060 t_140210 ON t.status = t_140210.id and (t_140210.at_3067 > 1)
		inner JOIN ks_ddlcontrol.exp001_zsl t_l22463 ON t.id = t_l22463.id_up and coalesce(t_l22463.iserror, 0) = 1
	) exp001 on exp001.реестр_счетов_id = d.id and exp001.реестр_законченных_случаев_id = sl.id

left outer join (-- 9.	Сопутствующие диагнозы (X-file)
		select 
		t_sl.id,
		string_agg(t_m001.mkb_code || ' - ' || t_m001_v.mkb_name, ';') as наименование
		from ks_ddlcontrol.sl t_sl  -- тч_записи_сведения_о_случае_рееестр_случаев
		inner join ks_ddlcontrol.sl_ds2_n sl_ds ON t_sl.id = sl_ds.id_up
		inner join ks_ddlcontrol.m001 t_m001 ON sl_ds.ds2_n_m001 = t_m001.id
		inner join ks_ddlcontrol.m001_vers t_m001_v ON t_m001.id = t_m001_v.id_up and coalesce(t_m001_v.datebeg,0) = (select max(coalesce(t_m001_v0.datebeg,0)) from ks_ddlcontrol.m001_vers t_m001_v0 
			where t_m001_v0.id_up = t_m001_v.id_up and coalesce(t_m001_v0.datebeg,0) <= coalesce(t_sl.date_1,0) )
		group by t_sl.id
) mkb on mkb.id = sl_rs.id

left outer join (-- 10.	Диагнозы осложнений
		select 
		t_sl.id,
		string_agg(t_m001.mkb_code || ' - ' || t_m001_v.mkb_name, ';') as наименование
		from ks_ddlcontrol.sl t_sl  -- тч_записи_сведения_о_случае_рееестр_случаев
		inner join ks_ddlcontrol.sl_ds3 sl_ds ON t_sl.id = sl_ds.id_up
		inner join ks_ddlcontrol.m001 t_m001 ON sl_ds.ds3_m001 = t_m001.id
		inner join ks_ddlcontrol.m001_vers t_m001_v ON t_m001.id = t_m001_v.id_up and coalesce(t_m001_v.datebeg,0) = (select max(coalesce(t_m001_v0.datebeg,0)) from ks_ddlcontrol.m001_vers t_m001_v0 
			where t_m001_v0.id_up = t_m001_v.id_up and coalesce(t_m001_v0.datebeg,0) <= coalesce(t_sl.date_1,0) )
		group by t_sl.id
) mkb_o on mkb.id = sl_rs.id;
raise notice '5. заполнили sl_pivot';
end;
--$$
$functionx$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_money_to_char(value numeric, d integer)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
declare
	mask varchar; 
begin
	mask := repeat('G999',cast(round(length(cast(abs(trunc(value)) as varchar))/3+1) as int))
							||'D'||repeat('9',d);
	
return ltrim(case when value<1000 then cast(round(value, d) as varchar)
			else
				to_char(value,
						case when left(mask, 1)='G' then right(mask, length(mask)-1) else mask end
						)
			end);
end;
$function$
;

CREATE OR REPLACE FUNCTION ks_ddlcontrol.f_trunc_numeric(value numeric)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
declare a_value numeric;
begin
    a_value = abs(coalesce(value,0));
    return case when value<0 then '-' else '' end || 
    		case when floor(a_value) = a_value then cast(floor(a_value) as text)
    			else reverse(cast(cast(reverse(cast(a_value as text)) as numeric) as text))
    		end ;
end;
$function$
;
