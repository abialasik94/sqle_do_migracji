--Doinstalowanie potrzebnych  rozszerzeń
CREATE EXTENSION IF NOT EXISTS unaccent;

--tabele ogólne

-- tymczasowa z historią wersji i datami końca wersji
drop table if exists migracja.crfop_tmp_historia_wersji;
create table migracja.crfop_tmp_historia_wersji as
with tab1 as (
select fa_id, fop_id, kod_organu, uzytkownik as uzyt_od, data_operacji wersja_od, rodzaj_operacji, ROW_NUMBER() OVER (PARTITION BY fop_id ORDER BY fop_id, data_operacji) as num
from _gdos_crfopdb_public.fop_archiwum),
tab2 as (
select fa_id, fop_id, kod_organu, uzytkownik, data_operacji wersja_od, rodzaj_operacji, ROW_NUMBER()  OVER (PARTITION BY fop_id ORDER BY fop_id, data_operacji) - 1 as num
from _gdos_crfopdb_public.fop_archiwum),
tab3 as (
select tab1.*, tab2.uzytkownik uzyt_do ,case when tab2.wersja_od is not null then tab2.wersja_od else current_date end wersja_do from tab1 left join tab2 on tab1.num = tab2.num and tab1.fop_id = tab2.fop_id)
select * from tab3;

-- pomocnicza dla aktów prawnych - publikator
drop table if exists migracja.crfop_tmp_arch_akt_prawny_miejsce_pub;
create table migracja.crfop_tmp_arch_akt_prawny_miejsce_pub as
select fa_id, ap_id, case 
	when lower(unaccent(miejsce_pub)) like '%dolnoslaskie%' then 5
	when lower(unaccent(miejsce_pub)) like '%kujawsko-pomorskie%' then 6
	when lower(unaccent(miejsce_pub)) like '%lodzkie%' then 7
	when lower(unaccent(miejsce_pub)) like '%lubelskie%' then 8
	when lower(unaccent(miejsce_pub)) like '%lubuskie%' then 9
	when lower(unaccent(miejsce_pub)) like '%malopolskie%' then 10
	when lower(unaccent(miejsce_pub)) like '%mazowieckie%' then 11
	when lower(unaccent(miejsce_pub)) like '%opolskie%' then 12
	when lower(unaccent(miejsce_pub)) like '%podkarpackie%' then 13
	when lower(unaccent(miejsce_pub)) like '%podlaskie%' then 14
	when lower(unaccent(miejsce_pub)) like '%pomorskie%' then 15
	when lower(unaccent(miejsce_pub)) like '%slaskie%' then 16
	when lower(unaccent(miejsce_pub)) like '%swietokrzyskie%' then 17
	when lower(unaccent(miejsce_pub)) like '%warminsko-mazurskie%' then 18
	when lower(unaccent(miejsce_pub)) like '%wielkopolskie%' then 19
	when lower(unaccent(miejsce_pub)) like '%zachodniopomorskie%' then 20
	when lower(miejsce_pub) like '%ustaw%' then 2
	when lower(miejsce_pub) like '%monitor polski%' or lower(miejsce_pub) like 'm.p.%' then 1
	when lower(miejsce_pub) like '%unii%' then 4
	when lower(miejsce_pub) like '%ministerstw%' or lower(miejsce_pub) like '% ministr%' then 3
end as id_slo_fop_dokument_publikator
from _gdos_crfopdb_public._akt_prawny;

drop table if exists migracja.crfop_tmp_akt_prawny_miejsce_pub;
create table migracja.crfop_tmp_akt_prawny_miejsce_pub as
select fop_id, ap_id, case 
	when lower(unaccent(miejsce_pub)) like '%dolnoslaskie%' then 5
	when lower(unaccent(miejsce_pub)) like '%kujawsko-pomorskie%' then 6
	when lower(unaccent(miejsce_pub)) like '%lodzkie%' then 7
	when lower(unaccent(miejsce_pub)) like '%lubelskie%' then 8
	when lower(unaccent(miejsce_pub)) like '%lubuskie%' then 9
	when lower(unaccent(miejsce_pub)) like '%malopolskie%' then 10
	when lower(unaccent(miejsce_pub)) like '%mazowieckie%' then 11
	when lower(unaccent(miejsce_pub)) like '%opolskie%' then 12
	when lower(unaccent(miejsce_pub)) like '%podkarpackie%' then 13
	when lower(unaccent(miejsce_pub)) like '%podlaskie%' then 14
	when lower(unaccent(miejsce_pub)) like '%pomorskie%' then 15
	when lower(unaccent(miejsce_pub)) like '%slaskie%' then 16
	when lower(unaccent(miejsce_pub)) like '%swietokrzyskie%' then 17
	when lower(unaccent(miejsce_pub)) like '%warminsko-mazurskie%' then 18
	when lower(unaccent(miejsce_pub)) like '%wielkopolskie%' then 19
	when lower(unaccent(miejsce_pub)) like '%zachodniopomorskie%' then 20
	when lower(miejsce_pub) like '%ustaw%' then 2
	when lower(miejsce_pub) like '%monitor polski%' or lower(miejsce_pub) like 'm.p.%' then 1
	when lower(miejsce_pub) like '%unii%' then 4
	when lower(miejsce_pub) like '%ministerstw%' or lower(miejsce_pub) like '% ministr%' then 3
end as id_slo_fop_dokument_publikator
from _gdos_crfopdb_public.akt_prawny;


--------------------------------------------------------------------------------------------------------------------
-- --mapowanie statusow
-- case when roboczy is false and zatwierdzony is false and zniesiony is false then 2
-- when roboczy is false and zatwierdzony is false and zniesiony is true  then 5
-- when roboczy is false and zatwierdzony is false and zniesiony is null  then 2
-- when roboczy is false and zatwierdzony is true and zniesiony is  false then 3
-- when roboczy is false and zatwierdzony is true and zniesiony is  null  then 3
-- when roboczy is true and zatwierdzony is false and zniesiony is false then 2
-- when roboczy is true and zatwierdzony is false and zniesiony is null then 2
-- when roboczy is true and zatwierdzony is true and zniesiony is null then 2 end
-------------------------------------------------------------------

--Uzupełnianie gestorów
truncate table imap.sys_gestorzy ;
insert into imap.sys_gestorzy (kod, klasa, nazwa, geom)
select distinct on (kodjednostki) kodjednostki as kod, 'WOJ' klasa, nazwajednostki, geom
from ref_prg.vm_a01_granice_wojewodztw;
insert into imap.sys_gestorzy (kod, klasa, nazwa, geom)
select distinct on (kodjednostki) kodjednostki as kod, 'GMI' klasa, nazwajednostki, geom
from ref_prg.vm_a03_granice_gmin;

DELETE FROM imap.fop_rel_fop_lokalizacja;
INSERT INTO imap.fop_rel_fop_lokalizacja (wersja_od, uzyt_od, wersja_do, gestor, fop_id, lokalizacja)
SELECT case when d.data_wprowadzenia is not null then d.data_wprowadzenia else d.data_modyfikacji end, d.imie_nazwisko, '9999-12-31 23:59:59.000'::timestamptz  wersja_do, d.kod_organu, b.fop_id, kod_gmi
FROM _gdos_crfopdb_public.fop_prg_gmi b
left join _gdos_crfopdb_public.fop_edycja d on b.fop_id = d.fop_id;

--unikalne fop_id i wygenerowane objectid + gotowa tabelka do mapowania, wyciągnięcie wartości z tabel tematycznych + fop aktualnych i archiwalnych

--z racji na wolne działanie rep pliki  na dole pliku 
-------------------------konwencja rozpisana na osobne tabele relacyjne na końcu ------------------------------------------



drop table if exists migracja.crfop_obszar_chron_krajobrazu_gotowy;
    create table migracja.crfop_obszar_chron_krajobrazu_gotowy as 
    with obszar_chron_krajobrazu_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._obszar_chron_krajobrazu)),
    obszar_chron_krajobrazu_ids_nowe as (
    select distinct ochk_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct ochk_id from _gdos_crfopdb_public.obszar_chron_krajobrazu))
    SELECT a.fop_id, null fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
    null uzyt_od, null uzyt_do, fe.kod_organu, b.guid, b.opis, opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.wartosc_przyrodnicza, case when st_geometrytype(b.geom) in ('ST_Point', 'ST_MultiPoint') then b.geom else null end as geom_pkt,
case when st_geometrytype(b.geom) in ('ST_Polygon', 'ST_MultiPolygon') then b.geom else null end as geom_pow, b.nadzorca
    FROM _gdos_crfopdb_public.obszar_chron_krajobrazu a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join obszar_chron_krajobrazu_ids_nowe e on a.ochk_id = e.ochk_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    union 
    SELECT a.fop_id, a.fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, d.kod_organu, b.guid, b.opis opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.wartosc_przyrodnicza, case when st_geometrytype(b.geom) in ('ST_Point', 'ST_MultiPoint') then b.geom else null end as geom_pkt,
case when st_geometrytype(b.geom) in ('ST_Polygon', 'ST_MultiPolygon') then b.geom else null end as geom_pow, b.nadzorca
    FROM _gdos_crfopdb_public._obszar_chron_krajobrazu a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join obszar_chron_krajobrazu_ids e on a.fop_id = e.fop_id; 
    delete from migracja.crfop_obszar_chron_krajobrazu_gotowy where fop_id in (
	select fop_id from migracja.crfop_obszar_chron_krajobrazu_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');

drop table if exists migracja.crfop_obszar_chron_krajobrazu_gestorzy;
create table migracja.crfop_obszar_chron_krajobrazu_gestorzy as
with przeciecia as (
select st_area(st_intersection(ock.geom_pow, woj.geom)) inters_area , ock.nowe_id, ock.inspire_id, woj.nazwa, woj.id 
from migracja.crfop_obszar_chron_krajobrazu_gotowy ock left join (select * from imap.sys_gestorzy where klasa = 'WOJ') woj
on st_intersects(ock.geom_pow, woj.geom)
where ock.geom_pow is not null and st_isvalid(ock.geom_pow) and ock.wersja_do = '9999-12-31 23:59:59.000')
select nowe_id, inspire_id, nazwa, id from przeciecia
where inters_area in (
select max(a.inters_area)
from przeciecia a
group by a.inspire_id)
union
select ock.nowe_id, ock.inspire_id, woj.nazwa, woj.id 
from migracja.crfop_obszar_chron_krajobrazu_gotowy ock left join (select * from imap.sys_gestorzy where klasa = 'WOJ') woj 
on st_intersects(ock.geom_pkt, woj.geom)
where ock.geom_pkt is not null and ock.wersja_do = '9999-12-31 23:59:59.000';

--mapowanie
DELETE FROM imap.fop_obszar_chronionego_krajobrazu;
INSERT INTO imap.fop_obszar_chronionego_krajobrazu (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, nazwa, data_utworzenia, 
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, wartoscprzyrodnicza, geometria_pkt, geometria_pow, zatw_wersja_od, zatw_wersja_do, gestor)
SELECT ock.nowe_id, ock.inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, ock.nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy, powierzchnia, wartosc_przyrodnicza, geom_pkt, geom_pow, wersja_od, wersja_do, g.id
FROM migracja.crfop_obszar_chron_krajobrazu_gotowy ock
left join migracja.crfop_obszar_chron_krajobrazu_gestorzy g on ock.inspire_id = g.inspire_id;

drop table if exists migracja.crfop_park_krajobrazowy_gotowy;
    create table migracja.crfop_park_krajobrazowy_gotowy as 
    with park_krajobrazowy_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._park_krajobrazowy )),
    park_krajobrazowy_ids_nowe as(
    select distinct pk_id, nextval('fop__tech_id_seq') objectid_from_sequence from
    (select distinct pk_id from _gdos_crfopdb_public.park_krajobrazowy))
    SELECT a.fop_id, null fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
    null uzyt_od, null uzyt_do, fe.kod_organu, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pk_id, 
case when a.plan_ochrony is true then 'T' when a.plan_ochrony is false then 'N' end as plan_ochrony, 
a.data_planu, a.cel_ochrony, a.otulina,
case when st_geometrytype(b.geom) in ('ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString') then st_buffer(b.geom, 1) when st_geometrytype(b.geom) in ('ST_Polygon', 'ST_MultiPolygon') then b.geom else null end as geom_pow
    FROM _gdos_crfopdb_public.park_krajobrazowy a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join park_krajobrazowy_ids_nowe e on a.pk_id = e.pk_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    union 
    SELECT a.fop_id, a.fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, d.kod_organu, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pk_id, 
case when a.plan_ochrony is true then 'T' when a.plan_ochrony is false then 'N' end as plan_ochrony, 
a.data_planu, a.cel_ochrony, a.otulina,
case when st_geometrytype(b.geom) in ('ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString') then st_buffer(b.geom, 1) when st_geometrytype(b.geom) in ('ST_Polygon', 'ST_MultiPolygon') then b.geom else null end as geom_pow
    FROM _gdos_crfopdb_public._park_krajobrazowy a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join park_krajobrazowy_ids e on a.fop_id = e.fop_id; 
    delete from migracja.crfop_park_krajobrazowy_gotowy where fop_id in (
	select fop_id from migracja.crfop_park_krajobrazowy_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');
	
drop table if exists migracja.crfop_park_krajobrazowy_gestorzy;
create table migracja.crfop_park_krajobrazowy_gestorzy as
with pk_przeciecia as (
select st_area(st_intersection(pk.geom_pow, woj.geom)) inters_area , pk.nowe_id id_pk, woj.nazwa, woj.id id_gestora, pk.geom_pow, 
st_intersection(pk.geom_pow, woj.geom), pk.inspire_id
from migracja.crfop_park_krajobrazowy_gotowy pk left join (select * from imap.sys_gestorzy where klasa = 'WOJ') woj
on st_intersects(pk.geom_pow, woj.geom)
where pk.geom_pow is not null and st_isvalid(pk.geom_pow) and pk.wersja_do = '9999-12-31 23:59:59.000')
select pk.nowe_id pk_id, pk_przeciecia.inters_area, pk_przeciecia.nazwa, pk_przeciecia.id_gestora
from migracja.crfop_park_krajobrazowy_gotowy pk join (select * from pk_przeciecia where inters_area > 500000) pk_przeciecia on pk.inspire_id = pk_przeciecia.inspire_id;

DELETE FROM imap.fop_park_krajobrazowy;
INSERT INTO imap.fop_park_krajobrazowy (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, celeochrony, czy_planochrony, data_planu, geometria_pow, zatw_wersja_od, zatw_wersja_do)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, kod_organu, nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy, powierzchnia, cel_ochrony, plan_ochrony, data_planu, geom_pow, wersja_od, wersja_do
FROM migracja.crfop_park_krajobrazowy_gotowy;




drop table if exists migracja.crfop_park_krajobrazowy_otulina_gotowy;
    create table migracja.crfop_park_krajobrazowy_otulina_gotowy as 
    with park_krajobrazowy_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._park_krajobrazowy )),
    park_krajobrazowy_ids_nowe as (
    select distinct pk_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct pk_id from _gdos_crfopdb_public.park_krajobrazowy))
    SELECT a.fop_id, f.nowe_id id_fop_park_krajobrazowy, null fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
    null uzyt_od, null uzyt_do, fe.kod_organu, b.guid, b.opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pk_id, a.opis_granicy_ot, a.powierzchnia_ot, a.geom_ot
    FROM _gdos_crfopdb_public.park_krajobrazowy a join (select distinct pk_id, nowe_id from migracja.crfop_park_krajobrazowy_gotowy where otulina is true and wersja_do = '9999-12-31 23:59:59.000') f on a.pk_id = f.pk_id
    join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join park_krajobrazowy_ids e on a.fop_id = e.fop_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    where a.otulina is true
    union 
    SELECT a.fop_id, f.nowe_id id_fop_park_krajobrazowy, a.fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, d.kod_organu, b.guid, b.opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pk_id, a.opis_granicy_ot, a.powierzchnia_ot, a.geom_ot
    FROM _gdos_crfopdb_public._park_krajobrazowy a join (select distinct pk_id, nowe_id from migracja.crfop_park_krajobrazowy_gotowy where otulina is true and wersja_do <> '9999-12-31 23:59:59.000') f on a.pk_id = f.pk_id
    join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join park_krajobrazowy_ids_nowe e on a.pk_id = e.pk_id
    where a.otulina is true;
    delete from migracja.crfop_park_krajobrazowy_otulina_gotowy where fop_id in (
	select fop_id from migracja.crfop_park_krajobrazowy_otulina_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');
	
DELETE FROM imap.fop_park_krajobrazowy_otulina;
INSERT INTO imap.fop_park_krajobrazowy_otulina (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, id_fop_park_krajobrazowy, geometria_pow, zatw_wersja_od, zatw_wersja_do)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, kod_organu, nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy_ot, powierzchnia_ot, id_fop_park_krajobrazowy, 
case when st_geometrytype(geom_ot) in ('ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString') then st_buffer(geom_ot, 1) when st_geometrytype(geom_ot) in ('ST_Polygon', 'ST_MultiPolygon') then geom_ot else null end
, wersja_od, wersja_do
FROM migracja.crfop_park_krajobrazowy_otulina_gotowy;

/*
drop table if exists migracja.crfop_park_krajobrazowy_plan_ochrony_gotowy;
    create table migracja.crfop_park_krajobrazowy_plan_ochrony_gotowy as 
    with park_krajobrazowy_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._park_krajobrazowy 
    union
    select distinct fop_id from _gdos_crfopdb_public.park_krajobrazowy))
    SELECT a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, null, b.guid, b.opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pk_id, a.data_planu, a.cel_ochrony
    FROM _gdos_crfopdb_public.park_krajobrazowy a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join migracja.crfop_tmp_historia_wersji d on b.fop_id = d.fop_id
    join park_krajobrazowy_ids e on a.fop_id = e.fop_id
    where a.plan_ochrony is true
    union 
    SELECT a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, null, b.guid, b.opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pk_id, a.data_planu, a.cel_ochrony
    FROM _gdos_crfopdb_public._park_krajobrazowy a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join park_krajobrazowy_ids e on a.fop_id = e.fop_id
    where a.plan_ochrony is true;
    delete from migracja.crfop_park_krajobrazowy_gotowy where fop_id in (
	select fop_id from migracja.crfop_park_krajobrazowy_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');
	*/

-- odtąd w dół sprawdzone poza otulinami (i może obszarami pn)

drop table if exists migracja.crfop_park_narodowy_gotowy;
    create table migracja.crfop_park_narodowy_gotowy as 
    with park_narodowy_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._park_narodowy 
    union
    select distinct fop_id from _gdos_crfopdb_public.park_narodowy))
    SELECT null fa_id, a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
    null uzyt_od, null uzyt_do, fe.kod_organu, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.geom, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, 
case when a.plan_ochrony is true then 'T' when a.plan_ochrony is false then 'N' end as plan_ochrony, a.data_planu, 
case when a.zadania_ochronne is true then 'T' when a.zadania_ochronne is false then 'N' end as zadania_ochronne, 
a.data_zadan, a.opis_granicy_ot, a.powierzchnia_ot, a.geom_ot, a.otulina
    FROM _gdos_crfopdb_public.park_narodowy a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join park_narodowy_ids e on a.fop_id = e.fop_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    union 
    SELECT a.fa_id, a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, d.kod_organu, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.geom, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, 
case when a.plan_ochrony is true then 'T' when a.plan_ochrony is false then 'N' end as plan_ochrony, a.data_planu, 
case when a.zadania_ochronne is true then 'T' when a.zadania_ochronne is false then 'N' end as zadania_ochronne, 
a.data_zadan, a.opis_granicy_ot, a.powierzchnia_ot, a.geom_ot, a.otulina
    FROM _gdos_crfopdb_public._park_narodowy a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join park_narodowy_ids e on a.fop_id = e.fop_id; 
    delete from migracja.crfop_park_narodowy_gotowy where fop_id in (
	select fop_id from migracja.crfop_park_narodowy_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');

insert into imap.sys_gestorzy (kod, klasa, nazwa, geom)
select distinct on (inspire_id) inspire_id as kod, 'PN' klasa, nazwa, geom
from migracja.crfop_park_narodowy_gotowy
where geom is not null and wersja_do = '9999-12-31 23:59:59.000 +0100' and inspire_id is not null;

DELETE FROM imap.fop_park_narodowy;
INSERT INTO imap.fop_park_narodowy (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, czy_planochrony, data_planu, czy_zadaniaochronne, data_zadanochronnych, geometria_pow, zatw_wersja_od, zatw_wersja_do, gestor)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, pn.nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy, powierzchnia, null plan_ochrony, data_planu, null zadania_ochronne, data_zadan,
case when st_geometrytype(pn.geom) in ('ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString') then st_buffer(pn.geom, 1) when st_geometrytype(pn.geom) in ('ST_Polygon', 'ST_MultiPolygon') then pn.geom else null end
, wersja_od, wersja_do, ge.id as kod_organu
FROM migracja.crfop_park_narodowy_gotowy pn
left join sys_gestorzy ge on pn.inspire_id = ge.kod;

drop table if exists migracja.crfop_park_narodowy_obszar_ochrony_gotowy;
    create table migracja.crfop_park_narodowy_obszar_ochrony_gotowy as 
    with park_narodowy_ids as (
    select distinct oo_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct oo_id from _gdos_crfopdb_public._obszar_ochrony )),
    park_narodowy_ids_nowe as (
    select distinct oo_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct oo_id from _gdos_crfopdb_public.obszar_ochrony))
    SELECT a.fop_id, null fa_id, a.nowe_id id_fop_park_narodowy, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
    null uzyt_od, null uzyt_do, b.guid, b.opis, b.opis_granicy, b.powierzchnia, fe.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, case when c.rodzaj_ochrony_id = 'OC' then 1 when c.rodzaj_ochrony_id = 'OK' then 2 when c.rodzaj_ochrony_id = 'OS' then 3 else null end as rodzaj_ochrony,
case when c.caly_obszar is true then 'T' when c.caly_obszar is false then 'N' end as caly_obszar, c.powierzchnia_och, c.geom
    FROM (select distinct pn_id, fop_id, nowe_id from migracja.crfop_park_narodowy_gotowy ) a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join _gdos_crfopdb_public.obszar_ochrony c on a.fop_id = c.fop_id
    join park_narodowy_ids e on c.oo_id = e.oo_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    union 
    SELECT a.fop_id, a.fa_id, a.nowe_id id_fop_park_narodowy, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, b.guid, b.opis, b.opis_granicy, b.powierzchnia, d.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, case when c.rodzaj_ochrony_id = 'OC' then 1 when c.rodzaj_ochrony_id = 'OK' then 2 when c.rodzaj_ochrony_id = 'OS' then 3 else null end as rodzaj_ochrony,
case when c.caly_obszar is true then 'T' when c.caly_obszar is false then 'N' end as caly_obszar, c.powierzchnia_och, c.geom
    FROM (select distinct pn_id, fop_id, nowe_id, fa_id from migracja.crfop_park_narodowy_gotowy ) a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join _gdos_crfopdb_public._obszar_ochrony c on a.fa_id = c.fa_id
    join park_narodowy_ids_nowe e on c.oo_id = e.oo_id;
    delete from migracja.crfop_park_narodowy_obszar_ochrony_gotowy where fop_id in (
	select fop_id from migracja.crfop_park_narodowy_obszar_ochrony_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');
	
DELETE FROM imap.fop_park_narodowy_obszar_ochrony;
INSERT INTO imap.fop_park_narodowy_obszar_ochrony (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, id_fop_park_narodowy, id_slo_fop_park_rodzaj_ochrony, czy_calyobszar, geometria_pow, zatw_wersja_od, zatw_wersja_do, gestor)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, nazwa, data_utworzenia, id_slo_fop_status, guid,
opis, opis_granicy, powierzchnia_och, id_fop_park_narodowy, rodzaj_ochrony, caly_obszar,
case when st_geometrytype(geom) in ('ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString') then st_buffer(geom, 1) when st_geometrytype(geom) in ('ST_Polygon', 'ST_MultiPolygon') then geom else null end
, wersja_od, wersja_do, kod_organu
FROM migracja.crfop_park_narodowy_obszar_ochrony_gotowy;

drop table if exists migracja.crfop_park_narodowy_otulina_gotowy;
    create table migracja.crfop_park_narodowy_otulina_gotowy as 
    with park_narodowy_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._park_narodowy)),
    park_narodowy_ids_nowe as(
    select distinct pn_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct pn_id from _gdos_crfopdb_public.park_narodowy))
    SELECT a.fop_id, null fa_id, f.nowe_id id_fop_park_narodowy, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
    null uzyt_od, null uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, fe.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, a.opis_granicy_ot, a.powierzchnia_ot, a.geom_ot
    FROM _gdos_crfopdb_public.park_narodowy a join (select distinct pn_id, nowe_id from migracja.crfop_park_narodowy_gotowy where otulina is true and wersja_do = '9999-12-31 23:59:59.000') f on a.pn_id = f.pn_id
    join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join park_narodowy_ids e on a.fop_id = e.fop_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    where a.otulina is true
    union 
    SELECT a.fop_id, a.fa_id, f.nowe_id id_fop_park_narodowy, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, d.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, a.opis_granicy_ot, a.powierzchnia_ot, a.geom_ot
    FROM _gdos_crfopdb_public._park_narodowy a join (select distinct pn_id, nowe_id from migracja.crfop_park_narodowy_gotowy where otulina is true and wersja_do <> '9999-12-31 23:59:59.000') f on a.pn_id = f.pn_id
    join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join park_narodowy_ids_nowe e on a.pn_id = e.pn_id
    where a.otulina is true; 
    delete from migracja.crfop_park_narodowy_otulina_gotowy where fop_id in (
	select fop_id from migracja.crfop_park_narodowy_otulina_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');
	
DELETE FROM imap.fop_park_narodowy_otulina;
INSERT INTO imap.fop_park_narodowy_otulina (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, id_fop_park_narodowy, geometria_pow, zatw_wersja_od, zatw_wersja_do, gestor)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy_ot, powierzchnia_ot, id_fop_park_narodowy, 
case when st_geometrytype(geom_ot) in ('ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString') then st_buffer(geom_ot, 1) when st_geometrytype(geom_ot) in ('ST_Polygon', 'ST_MultiPolygon') then geom_ot else null end
, wersja_od, wersja_do, kod_organu
FROM migracja.crfop_park_narodowy_otulina_gotowy;

/*
drop table if exists migracja.crfop_park_narodowy_plan_ochrony_gotowy;
    create table migracja.crfop_park_narodowy_plan_ochrony_gotowy as 
    with park_narodowy_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._park_narodowy 
    union
    select distinct fop_id from _gdos_crfopdb_public.park_narodowy))
    SELECT a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, null, b.guid, b.opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, a.data_zadan
    FROM _gdos_crfopdb_public.park_narodowy a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join migracja.crfop_tmp_historia_wersji d on b.fop_id = d.fop_id
    join park_narodowy_ids e on a.fop_id = e.fop_id
    where a.plan_ochrony is true; 
    union 
    SELECT a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, null, b.guid, b.opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, a.data_zadan
    FROM _gdos_crfopdb_public._park_narodowy a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join park_narodowy_ids e on a.fop_id = e.fop_id
    where a.plan_ochrony is true; 
    delete from migracja.crfop_park_narodowy_gotowy where fop_id in (
	select fop_id from migracja.crfop_park_narodowy_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');


drop table if exists migracja.crfop_park_narodowy_zadania_ochronne_gotowy;
    create table migracja.crfop_park_narodowy_zadania_ochronne_gotowy as 
    with park_narodowy_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._park_narodowy 
    union
    select distinct fop_id from _gdos_crfopdb_public.park_narodowy))
    SELECT a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, null, b.guid, b.opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, a.data_zadan
    FROM _gdos_crfopdb_public.park_narodowy a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join migracja.crfop_tmp_historia_wersji d on b.fop_id = d.fop_id
    join park_narodowy_ids e on a.fop_id = e.fop_id
    where a.zadania_ochronne is true; 
    union 
    SELECT a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, null, b.guid, b.opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pn_id, a.data_zadan
    FROM _gdos_crfopdb_public._park_narodowy a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join park_narodowy_ids e on a.fop_id = e.fop_id
    where a.zadania_ochronne is true; 
    delete from migracja.crfop_park_narodowy_gotowy where fop_id in (
	select fop_id from migracja.crfop_park_narodowy_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');
*/

drop table if exists migracja.crfop_pomnik_przyrody_gotowy;
    create table migracja.crfop_pomnik_przyrody_gotowy as 
    with pomnik_przyrody_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._pomnik_przyrody)),
    pomnik_przyrody_ids_nowe as (
    select distinct pp_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct pp_id from _gdos_crfopdb_public.pomnik_przyrody))
    SELECT a.fop_id, null fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
    null uzyt_od, null uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, fe.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pp_id, a.opis_lokalizacji, 
case when a.typ_tworu_id in (1, 2, 4) then 2 when a.typ_tworu_id = 3 then 1 end as typ_pomnika, 
case when a.typ_tworu_id in (1, 2) then a.typ_tworu_id end as podtyp_pomnika,
f.rodzaj_tworu_id as rodzaj_pomnika, b.geom
    FROM _gdos_crfopdb_public.pomnik_przyrody a left join (select distinct pp_id, rodzaj_tworu_id from _gdos_crfopdb_public.twor_przyrody where rodzaj_tworu_id is not null) f on a.pp_id = f.pp_id    
    join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join pomnik_przyrody_ids_nowe e on a.pp_id = e.pp_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    union 
    SELECT a.fop_id, a.fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, d.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.pp_id, a.opis_lokalizacji, 
case when a.typ_tworu_id in (1, 2, 4) then 2 when a.typ_tworu_id = 3 then 1 end as typ_pomnika, 
case when a.typ_tworu_id in (1, 2) then a.typ_tworu_id end as podtyp_pomnika,
f.rodzaj_tworu_id as rodzaj_pomnika, b.geom
    FROM _gdos_crfopdb_public._pomnik_przyrody a left join (select distinct pp_id, rodzaj_tworu_id from _gdos_crfopdb_public._twor_przyrody where rodzaj_tworu_id is not null) f on a.pp_id = f.pp_id    
    join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
   	join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join pomnik_przyrody_ids e on a.fop_id = e.fop_id; 
    delete from migracja.crfop_pomnik_przyrody_gotowy where fop_id in (
	select fop_id from migracja.crfop_pomnik_przyrody_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');
	
drop table if exists migracja.crfop_pomnik_przyrody_gestorzy;
create table migracja.crfop_pomnik_przyrody_gestorzy as
with pp_przeciecia as (
select st_area(st_intersection(pp.geom, gmi.geom)) inters_area , pp.nowe_id id_pp, gmi.nazwa, gmi.id id_gestora, pp.geom, 
st_intersection(pp.geom, gmi.geom), pp.inspire_id
from migracja.crfop_pomnik_przyrody_gotowy pp left join (select * from imap.sys_gestorzy where klasa = 'GMI') gmi
on st_intersects(pp.geom, gmi.geom)
where pp.geom is not null and st_isvalid(pp.geom) and pp.wersja_do = '9999-12-31 23:59:59.000')
select distinct pp.nowe_id pp_id, pp_przeciecia.inters_area, pp_przeciecia.nazwa, pp_przeciecia.id_gestora, st_intersection
from migracja.crfop_pomnik_przyrody_gotowy pp join (select * from pp_przeciecia) pp_przeciecia on pp.inspire_id = pp_przeciecia.inspire_id;

DELETE FROM imap.fop_pomnik_przyrody;
INSERT INTO imap.fop_pomnik_przyrody (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, id_slo_fop_pomnik_rodzaj, id_slo_fop_pomnik_typ, id_slo_fop_pomnik_podtyp, 
geometria_pkt, geometria_pow, zatw_wersja_od, zatw_wersja_do, gestor)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy, powierzchnia, rodzaj_pomnika::int, typ_pomnika, podtyp_pomnika,
case when st_geometrytype(geom) in ('ST_Point', 'ST_MultiPoint') then geom else null end as geom_pkt,
case when st_geometrytype(geom) in ('ST_Polygon', 'ST_MultiPolygon') then geom else null end as geom_pow, wersja_od, wersja_do, kod_organu
FROM migracja.crfop_pomnik_przyrody_gotowy;

drop table if exists migracja.crfop_twor_przyrody_gotowy;
    create table migracja.crfop_twor_przyrody_gotowy as 
    with twor_przyrody_ids as (
    select distinct tp_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct a.tp_id from _gdos_crfopdb_public.twor_przyrody a))
    SELECT f.fop_id, f.nowe_id id_fop_pomnik_przyrody, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
    null uzyt_od, null uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, fe.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.tp_id, a.liczba_tworow, a.gatunek_drzewa_id, a.obwod, a.piersnica, a.wysokosc_drzewa, a.geom
    FROM _gdos_crfopdb_public.twor_przyrody a join 
    (select distinct pp_id, fop_id, nowe_id from migracja.crfop_pomnik_przyrody_gotowy where wersja_do = '9999-12-31 23:59:59.000') f on a.pp_id = f.pp_id 
    join twor_przyrody_ids e on a.tp_id = e.tp_id join _gdos_crfopdb_public.fop b on f.fop_id = b.fop_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    ;
    /*union 
    SELECT f.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, null, b.guid, b.opis, b.opis_granicy, b.powierzchnia, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.tp_id, a.liczba_tworow, null, null, null, null, a.geom
    FROM _gdos_crfopdb_public._twor_przyrody a join _gdos_crfopdb_public._pomnik_przyrody f on a.pp_id = f.pp_id join _gdos_crfopdb_public._fop b on f.fop_id = b.fop_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join twor_przyrody_ids e on f.fop_id = e.fop_id; */
delete from migracja.crfop_twor_przyrody_gotowy where fop_id in (
select fop_id from migracja.crfop_twor_przyrody_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');
	
DELETE FROM imap.fop_pomnik_przyrody_twor;
INSERT INTO imap.fop_pomnik_przyrody_twor (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, id_fop_pomnik_przyrody, liczbatworow, id_slo_pomnik_drzewo_gatunek, 
drzewoobwod, drzewopiersnica, drzewowysokosc, geometria_pkt, geometria_pow, zatw_wersja_od, zatw_wersja_do, gestor)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy, powierzchnia, id_fop_pomnik_przyrody, liczba_tworow, gatunek_drzewa_id, obwod, piersnica, wysokosc_drzewa, 
case when st_geometrytype(geom) in ('ST_Point', 'ST_MultiPoint') then geom else null end as geom_pkt,
case when st_geometrytype(geom) in ('ST_Polygon', 'ST_MultiPolygon') then geom else null end as geom_pow, wersja_od, wersja_do, kod_organu
FROM migracja.crfop_twor_przyrody_gotowy;


drop table if exists migracja.crfop_stanowisko_dokumentacyjne_gotowy;
    create table migracja.crfop_stanowisko_dokumentacyjne_gotowy as 
    with stanowisko_dokumentacyjne_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._stanowisko_dokumentacyjne 
    union
    select distinct fop_id from _gdos_crfopdb_public.stanowisko_dokumentacyjne))
    SELECT a.fop_id, null fa_id, b.geom, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'  wersja_do,
    null uzyt_od, null uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, fe.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.sd_id, a.cel_ochrony, a.charakter_geolog, a.kategoria_stan_id, a.rodzaj_stan_id
    FROM _gdos_crfopdb_public.stanowisko_dokumentacyjne a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join migracja.crfop_tmp_historia_wersji d on b.fop_id = d.fop_id
    join stanowisko_dokumentacyjne_ids e on a.fop_id = e.fop_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    union 
    SELECT a.fop_id, a.fa_id, b.geom, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, d.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.sd_id, a.cel_ochrony, a.charakter_geolog, a.kategoria_stan_id, a.rodzaj_stan_id
    FROM _gdos_crfopdb_public._stanowisko_dokumentacyjne a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join stanowisko_dokumentacyjne_ids e on a.fop_id = e.fop_id; 
    delete from migracja.crfop_stanowisko_dokumentacyjne_gotowy where fop_id in (
	select fop_id from migracja.crfop_stanowisko_dokumentacyjne_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');

drop table if exists migracja.crfop_stanowisko_dokumentacyjne_gestorzy;
create table migracja.crfop_stanowisko_dokumentacyjne_gestorzy as
with przeciecia as (
select st_area(st_intersection(sd.geom, woj.geom)) inters_area , sd.nowe_id, sd.inspire_id, woj.nazwa, woj.id 
from migracja.crfop_stanowisko_dokumentacyjne_gotowy sd left join (select * from imap.sys_gestorzy where klasa = 'GMI') woj
on st_intersects(sd.geom, woj.geom)
where sd.geom is not null and st_isvalid(sd.geom) and sd.wersja_do = '9999-12-31 23:59:59.000')
select nowe_id, inspire_id, nazwa, id from przeciecia
where inters_area in (
select max(a.inters_area)
from przeciecia a
group by a.inspire_id)
union
select sd.nowe_id, sd.inspire_id, woj.nazwa, woj.id 
from migracja.crfop_stanowisko_dokumentacyjne_gotowy sd left join (select * from imap.sys_gestorzy where klasa = 'GMI') woj 
on st_intersects(sd.geom, woj.geom)
where sd.geom is not null and sd.wersja_do = '9999-12-31 23:59:59.000' and sd.inspire_id = 'PL.ZIPOP.1393.SD.17';

DELETE FROM imap.fop_stanowisko_dokumentacyjne;
INSERT INTO imap.fop_stanowisko_dokumentacyjne (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, celeochrony, charakterystykageologiczna, id_slo_fop_stanowisko_kategoria, 
id_slo_fop_stanowisko_rodzaj, geometria_pow, zatw_wersja_od, zatw_wersja_do, gestor)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy, powierzchnia, cel_ochrony, charakter_geolog, kategoria_stan_id, rodzaj_stan_id,
case when st_geometrytype(geom) in ('ST_Polygon', 'ST_MultiPolygon') then geom else null end as geom_pow, wersja_od, wersja_do, kod_organu
FROM migracja.crfop_stanowisko_dokumentacyjne_gotowy;


drop table if exists migracja.crfop_uzytek_ekologiczny_gotowy;
    create table migracja.crfop_uzytek_ekologiczny_gotowy as 
    with uzytek_ekologiczny_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._uzytek_ekologiczny 
    union
    select distinct fop_id from _gdos_crfopdb_public.uzytek_ekologiczny))
    SELECT a.fop_id, null fa_id, b.geom, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'  wersja_do,
    null uzyt_od, null uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, fe.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.ue_id, a.cel_ochrony, a.wartosc_przyrodnicza, a.rodzaj_uzytku_id
    FROM _gdos_crfopdb_public.uzytek_ekologiczny a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join uzytek_ekologiczny_ids e on a.fop_id = e.fop_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    union 
    SELECT a.fop_id, a.fa_id, b.geom, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
    d.uzyt_od, d.uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, d.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.ue_id, a.cel_ochrony, a.wartosc_przyrodnicza, a.rodzaj_uzytku_id
    FROM _gdos_crfopdb_public._uzytek_ekologiczny a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join uzytek_ekologiczny_ids e on a.fop_id = e.fop_id; 
    delete from migracja.crfop_uzytek_ekologiczny_gotowy where fop_id in (
	select fop_id from migracja.crfop_uzytek_ekologiczny_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');
	
drop table if exists migracja.crfop_uzytek_ekologiczny_gestorzy;
create table migracja.crfop_uzytek_ekologiczny_gestorzy as
with ue_przeciecia as (
select st_area(st_intersection(ue.geom, gmi.geom)) inters_area , ue.nowe_id id_ue, gmi.nazwa, gmi.id id_gestora, ue.geom, 
st_intersection(ue.geom, gmi.geom), ue.inspire_id
from migracja.crfop_uzytek_ekologiczny_gotowy ue left join (select * from imap.sys_gestorzy where klasa = 'GMI') gmi
on st_intersects(ue.geom, gmi.geom)
where ue.geom is not null and st_isvalid(ue.geom) and ue.wersja_do = '9999-12-31 23:59:59.000')
select distinct ue.nowe_id ue_id, ue_przeciecia.inters_area, ue_przeciecia.nazwa, ue_przeciecia.id_gestora, st_intersection, ue.inspire_id
from migracja.crfop_uzytek_ekologiczny_gotowy ue join (select * from ue_przeciecia where inters_area > 5) ue_przeciecia on ue.inspire_id = ue_przeciecia.inspire_id;


DELETE FROM imap.fop_uzytek_ekologiczny;
INSERT INTO imap.fop_uzytek_ekologiczny (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, celeochrony, wartoscprzyrodnicza, 
id_slo_fop_uzytek_rodzaj, geometria_pow, zatw_wersja_od, zatw_wersja_do, gestor)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy, powierzchnia, cel_ochrony, wartosc_przyrodnicza, rodzaj_uzytku_id,
case when st_geometrytype(geom) in ('ST_Polygon', 'ST_MultiPolygon') then geom else null end as geom_pow, wersja_od, wersja_do, kod_organu
FROM migracja.crfop_uzytek_ekologiczny_gotowy;

drop table if exists migracja.crfop_zespol_przyr_kraj_gotowy;
    create table migracja.crfop_zespol_przyr_kraj_gotowy as 
    with zespol_przyr_kraj_ids as (
    select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
    (select distinct fop_id from _gdos_crfopdb_public._zespol_przyr_kraj 
    union
    select distinct fop_id from _gdos_crfopdb_public.zespol_przyr_kraj))
    SELECT a.fop_id, null fa_id, b.geom, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
    null uzyt_od, null uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, fe.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.zpk_id, a.cel_ochrony, a.wartosc_przyrodnicza
    FROM _gdos_crfopdb_public.zespol_przyr_kraj a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
    join zespol_przyr_kraj_ids e on a.fop_id = e.fop_id
    left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
    union 
    SELECT a.fop_id, a.fa_id, b.geom, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od  wersja_od, d.wersja_do  wersja_do,
    d.uzyt_od uzyt_od, d.uzyt_do uzyt_do, b.guid, concat(b.opis, ', nadzorca: ', b.nadzorca) opis, b.opis_granicy, b.powierzchnia, d.kod_organu, case when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is true  then 5
when b.roboczy is false and b.zatwierdzony is false and b.zniesiony is null  then 2
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  false then 3
when b.roboczy is false and b.zatwierdzony is true and b.zniesiony is  null  then 3
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is false then 2
when b.roboczy is true and b.zatwierdzony is false and b.zniesiony is null then 2
when b.roboczy is true and b.zatwierdzony is true and b.zniesiony is null then 2 end as id_slo_fop_status, a.zpk_id, a.cel_ochrony, a.wartosc_przyrodnicza
    FROM _gdos_crfopdb_public._zespol_przyr_kraj a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
    join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
    join zespol_przyr_kraj_ids e on a.fop_id = e.fop_id; 
    delete from migracja.crfop_zespol_przyr_kraj_gotowy where fop_id in (
	select fop_id from migracja.crfop_zespol_przyr_kraj_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');

drop table if exists migracja.crfop_zespol_przyr_kraj_gestorzy;
create table migracja.crfop_zespol_przyr_kraj_gestorzy as
with zpk_przeciecia as (
select st_area(st_intersection(zpk.geom, gmi.geom)) inters_area , zpk.nowe_id id_zpk, gmi.nazwa, gmi.id id_gestora, zpk.geom, 
st_intersection(zpk.geom, gmi.geom), zpk.inspire_id
from migracja.crfop_zespol_przyr_kraj_gotowy zpk left join (select * from imap.sys_gestorzy where klasa = 'GMI') gmi
on st_intersects(zpk.geom, gmi.geom)
where zpk.geom is not null and st_isvalid(zpk.geom) and zpk.wersja_do = '9999-12-31 23:59:59.000')
select distinct zpk.nowe_id zpk_id, zpk_przeciecia.inters_area, zpk_przeciecia.nazwa, zpk_przeciecia.id_gestora, st_intersection, zpk.inspire_id
from migracja.crfop_zespol_przyr_kraj_gotowy zpk join (select * from zpk_przeciecia where inters_area > 10) zpk_przeciecia on zpk.inspire_id = zpk_przeciecia.inspire_id;


DELETE FROM imap.fop_zespol_przyrodniczo_krajobrazowy;
INSERT INTO imap.fop_zespol_przyrodniczo_krajobrazowy (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, nazwa, data_utworzenia,
id_slo_fop_status, idguid, opis, opisprzebiegugranicy, powierzchnia, celeochrony, wartoscprzyrodnicza, geometria_pkt,
geometria_pow, zatw_wersja_od, zatw_wersja_do, gestor)
SELECT nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, nazwa, data_utworzenia, id_slo_fop_status, guid, 
opis, opis_granicy, powierzchnia, cel_ochrony, wartosc_przyrodnicza,
case when st_geometrytype(geom) in ('ST_Point', 'ST_MultiPoint') then geom else null end as geom_pkt,
case when st_geometrytype(geom) in ('ST_Polygon', 'ST_MultiPolygon') then geom else null end as geom_pow, wersja_od, wersja_do, kod_organu
FROM migracja.crfop_zespol_przyr_kraj_gotowy;
	
drop table if exists migracja.crfop_konwencja_gotowy;
create table migracja.crfop_konwencja_gotowy as 
with konwencja_ids as (
select distinct fop_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
(select distinct fop_id from _gdos_crfopdb_public._konwencja)),
konwencja_ids_nowe as (
select distinct ko_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
(select distinct ko_id from _gdos_crfopdb_public.konwencja))
SELECT a.fop_id, null fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
null uzyt_od, null uzyt_do, null, a.rodzaj_konw_id, 
case when a.caly_obszar is true then 'T' when a.caly_obszar is false then 'N' end as caly_obszar, 
case when st_geometrytype(b.geom) in ('ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString') then st_buffer(b.geom, 1) when st_geometrytype(b.geom) in ('ST_Polygon', 'ST_MultiPolygon') then b.geom else null end as geom_pow
FROM _gdos_crfopdb_public.konwencja a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
join migracja.crfop_tmp_historia_wersji d on b.fop_id = d.fop_id
join konwencja_ids_nowe e on a.ko_id = e.ko_id
union 
SELECT a.fop_id, a.fa_id, b.roboczy, b.zatwierdzony, b.zniesiony, b.nazwa, b.data_utworzenia, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
d.uzyt_od, d.uzyt_do, null, a.rodzaj_konw_id, 
case when a.caly_obszar is true then 'T' when a.caly_obszar is false then 'N' end as caly_obszar,
case when st_geometrytype(b.geom) in ('ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString') then st_buffer(b.geom, 1) when st_geometrytype(b.geom) in ('ST_Polygon', 'ST_MultiPolygon') then b.geom else null end as geom_pow
FROM _gdos_crfopdb_public._konwencja a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
join konwencja_ids e on a.fop_id = e.fop_id; 
delete from migracja.crfop_konwencja_gotowy where fop_id in (
select fop_id from migracja.crfop_konwencja_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');

--mapowanie
DELETE FROM imap.fop_obszar_chronionego_krajobrazu_konwencja;
INSERT INTO imap.fop_obszar_chronionego_krajobrazu_konwencja (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_obszar_chronionego_krajobrazu, id_slo_fop_konwencja, nazwa, data_objecia, czy_calyobszar, geometria_pow)
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
ock.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_obszar_chron_krajobrazu_gotowy ock on ko.fa_id = ock.fa_id
union
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
ock.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko join 
migracja.crfop_obszar_chron_krajobrazu_gotowy ock on ko.fop_id = ock.fop_id
where ko.wersja_do = '9999-12-31 23:59:59.000 +0100' and ock.wersja_do = '9999-12-31 23:59:59.000 +0100';


DELETE FROM imap.fop_park_krajobrazowy_konwencja;
INSERT INTO imap.fop_park_krajobrazowy_konwencja (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_park_krajobrazowy, id_slo_fop_konwencja, nazwa, data_objecia, czy_calyobszar, geometria_pow)
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
pk.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_park_krajobrazowy_gotowy pk on ko.fa_id = pk.fa_id
union
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
pk.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_park_krajobrazowy_gotowy pk on ko.fop_id = pk.fop_id
where ko.wersja_do = '9999-12-31 23:59:59.000 +0100' and pk.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_park_narodowy_konwencja;
INSERT INTO imap.fop_park_narodowy_konwencja (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_park_narodowy, id_slo_fop_konwencja, nazwa, data_objecia, czy_calyobszar, geometria_pow)
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
pn.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_park_narodowy_gotowy pn on ko.fa_id = pn.fa_id
union
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
pn.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_park_narodowy_gotowy pn on ko.fop_id = pn.fop_id
where ko.wersja_do = '9999-12-31 23:59:59.000 +0100' and pn.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_pomnik_przyrody_konwencja;
INSERT INTO imap.fop_pomnik_przyrody_konwencja (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_pomnik_przyrody, id_slo_fop_konwencja, nazwa, data_objecia, czy_calyobszar, geometria_pow)
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
pp.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_pomnik_przyrody_gotowy pp on ko.fa_id = pp.fa_id
union
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
pp.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_pomnik_przyrody_gotowy pp on ko.fop_id = pp.fop_id
where ko.wersja_do = '9999-12-31 23:59:59.000 +0100' and pp.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_stanowisko_dokumentacyjne_konwencja;
INSERT INTO imap.fop_stanowisko_dokumentacyjne_konwencja (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_stanowisko_dokumentacyjne, id_slo_fop_konwencja, nazwa, data_objecia, czy_calyobszar, geometria_pow)
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
sd.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_stanowisko_dokumentacyjne_gotowy sd on ko.fa_id = sd.fa_id
union 
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
sd.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_stanowisko_dokumentacyjne_gotowy sd on ko.fop_id = sd.fop_id
where ko.wersja_do = '9999-12-31 23:59:59.000 +0100' and sd.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_uzytek_ekologiczny_konwencja;
INSERT INTO imap.fop_uzytek_ekologiczny_konwencja (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_uzytek_ekologiczny, id_slo_fop_konwencja, nazwa, data_objecia, czy_calyobszar, geometria_pow)
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
ue.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_uzytek_ekologiczny_gotowy ue on ko.fa_id = ue.fa_id
union 
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
ue.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_uzytek_ekologiczny_gotowy ue on ko.fop_id = ue.fop_id
where ko.wersja_do = '9999-12-31 23:59:59.000 +0100' and ue.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_zespol_przyrodniczo_krajobrazowy_konwencja;
INSERT INTO imap.fop_zespol_przyrodniczo_krajobrazowy_konwencja (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_zespol_przyrodniczo_krajobrazowy, id_slo_fop_konwencja, nazwa, data_objecia, czy_calyobszar, geometria_pow)
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
zpk.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_zespol_przyr_kraj_gotowy zpk on ko.fa_id = zpk.fa_id
union 
SELECT ko.nowe_id, ko.inspire_id, ko.wersja_od, ko.wersja_do, ko.wersja_od, ko.uzyt_od, ko.uzyt_do, null, 
zpk.nowe_id ,ko.rodzaj_konw_id, ko.nazwa, ko.data_utworzenia, ko.caly_obszar, ko.geom_pow
from migracja.crfop_konwencja_gotowy ko
inner join migracja.crfop_zespol_przyr_kraj_gotowy zpk on ko.fop_id = zpk.fop_id
where ko.wersja_do = '9999-12-31 23:59:59.000 +0100' and zpk.wersja_do = '9999-12-31 23:59:59.000 +0100';

drop table if exists migracja.crfop_akt_prawny_gotowy;
create table migracja.crfop_akt_prawny_gotowy as 
with akt_prawny_ids as (
select distinct ap_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
(select distinct ap_id from _gdos_crfopdb_public._akt_prawny
union
select distinct ap_id from _gdos_crfopdb_public.akt_prawny))
--akt_prawny_ids_nowe as (
--select distinct ap_id, nextval('fop__tech_id_seq') objectid_from_sequence from 
--(select distinct ap_id from _gdos_crfopdb_public.akt_prawny))
SELECT a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, null rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, current_date wersja_od, '9999-12-31 23:59:59.000'::timestamptz  wersja_do,
null uzyt_od, null uzyt_do, null, a.nazwa, a.oznaczenie, a.data_pub, a.obowiazuje_do, a.link, a.ap_id, null fa_id, fe.kod_organu, mp.id_slo_fop_dokument_publikator, 
case when mp.id_slo_fop_dokument_publikator is null then 'niezmapowany publikator: '|| a.miejsce_pub end as uwagi
FROM _gdos_crfopdb_public.akt_prawny a join _gdos_crfopdb_public.fop b on a.fop_id = b.fop_id
join akt_prawny_ids e on a.ap_id = e.ap_id
left join _gdos_crfopdb_public.fop_edycja fe on fe.fop_id = b.fop_id
left join migracja.crfop_tmp_akt_prawny_miejsce_pub mp on a.ap_id = mp.ap_id and a.fop_id = mp.fop_id
union 
SELECT a.fop_id, b.roboczy, b.zatwierdzony, b.zniesiony, d.rodzaj_operacji, e.objectid_from_sequence nowe_id, b.inspire_id, d.wersja_od, d.wersja_do,
d.uzyt_od, d.uzyt_do, null, a.nazwa, a.oznaczenie, a.data_pub, a.obowiazuje_do, a.link, a.ap_id, a.fa_id, d.kod_organu, mp.id_slo_fop_dokument_publikator, 
case when mp.id_slo_fop_dokument_publikator is null then 'niezmapowany publikator: '|| a.miejsce_pub end as uwagi
FROM _gdos_crfopdb_public._akt_prawny a join _gdos_crfopdb_public._fop b on  a.fa_id = b.fa_id
join migracja.crfop_tmp_historia_wersji d on b.fa_id = d.fa_id
left join migracja.crfop_tmp_arch_akt_prawny_miejsce_pub mp on a.ap_id = mp.ap_id and a.fa_id = mp.fa_id
join akt_prawny_ids e on a.ap_id = e.ap_id;

-- usunięcie z migracji danych usuniętych niezatwierdzonych
delete from migracja.crfop_akt_prawny_gotowy where fop_id in (
	select fop_id from migracja.crfop_akt_prawny_gotowy where zatwierdzony is true and rodzaj_operacji = 'usunięcie');

DELETE FROM imap.fop_dokument;
INSERT INTO imap.fop_dokument (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, tytul, oznaczeniepublikatora, data_publikacji,
data_obowiazywania, id_slo_fop_dokument_publikator, uwagi)
select nowe_id, inspire_id, wersja_od, wersja_do, wersja_od, uzyt_od, uzyt_do, kod_organu, nazwa, 
oznaczenie, data_pub, obowiazuje_do, id_slo_fop_dokument_publikator, uwagi
from migracja.crfop_akt_prawny_gotowy;

DELETE FROM imap.fop_obszar_chronionego_krajobrazu_rel_dokument;
INSERT INTO imap.fop_obszar_chronionego_krajobrazu_rel_dokument (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, 
id_fop_obszar_chronionego_krajobrazu, id_fop_dokument)
select ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, ock.nowe_id, ap.nowe_id 
from migracja.crfop_akt_prawny_gotowy ap 
inner join migracja.crfop_obszar_chron_krajobrazu_gotowy ock on ap.fa_id = ock.fa_id
union 
select ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, ock.nowe_id, ap.nowe_id 
from migracja.crfop_akt_prawny_gotowy ap 
inner join migracja.crfop_obszar_chron_krajobrazu_gotowy ock on ap.fop_id = ock.fop_id
where ap.wersja_do = '9999-12-31 23:59:59.000 +0100' and ock.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_park_krajobrazowy_rel_dokument;
INSERT INTO imap.fop_park_krajobrazowy_rel_dokument (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_park_krajobrazowy, id_fop_dokument)
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
pk.nowe_id ,ap.nowe_id
from migracja.crfop_konwencja_gotowy ap
inner join migracja.crfop_park_krajobrazowy_gotowy pk on ap.fa_id = pk.fa_id
union
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
pk.nowe_id ,ap.nowe_id
from migracja.crfop_konwencja_gotowy ap
inner join migracja.crfop_park_krajobrazowy_gotowy pk on ap.fop_id = pk.fop_id
where ap.wersja_do = '9999-12-31 23:59:59.000 +0100' and pk.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_park_narodowy_rel_dokument;
INSERT INTO imap.fop_park_narodowy_rel_dokument (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_park_narodowy, id_fop_dokument)
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
pn.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_park_narodowy_gotowy pn on ap.fa_id = pn.fa_id
union
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
pn.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_park_narodowy_gotowy pn on ap.fop_id = pn.fop_id
where ap.wersja_do = '9999-12-31 23:59:59.000 +0100' and pn.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_pomnik_przyrody_rel_dokument;
INSERT INTO imap.fop_pomnik_przyrody_rel_dokument (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_pomnik_przyrody, id_fop_dokument)
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
pp.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_pomnik_przyrody_gotowy pp on ap.fa_id = pp.fa_id
union
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
pp.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_pomnik_przyrody_gotowy pp on ap.fop_id = pp.fop_id
where ap.wersja_do = '9999-12-31 23:59:59.000 +0100' and pp.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_stanowisko_dokumentacyjne_rel_dokument;
INSERT INTO imap.fop_stanowisko_dokumentacyjne_rel_dokument (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_stanowisko_dokumentacyjne, id_fop_dokument)
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
sd.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_stanowisko_dokumentacyjne_gotowy sd on ap.fa_id = sd.fa_id
union 
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
sd.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_stanowisko_dokumentacyjne_gotowy sd on ap.fop_id = sd.fop_id
where ap.wersja_do = '9999-12-31 23:59:59.000 +0100' and sd.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_uzytek_ekologiczny_rel_dokument;
INSERT INTO imap.fop_uzytek_ekologiczny_rel_dokument (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_uzytek_ekologiczny, id_fop_dokument)
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
ue.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_uzytek_ekologiczny_gotowy ue on ap.fa_id = ue.fa_id
union 
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
ue.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_uzytek_ekologiczny_gotowy ue on ap.fop_id = ue.fop_id
where ap.wersja_do = '9999-12-31 23:59:59.000 +0100' and ue.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_zespol_przyrodniczo_krajobrazowy_rel_dokument;
INSERT INTO imap.fop_zespol_przyrodniczo_krajobrazowy_rel_dokument (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, gestor, 
id_fop_zespol_przyrodniczo_krajobrazowy, id_fop_dokument)
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
zpk.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_zespol_przyr_kraj_gotowy zpk on ap.fa_id = zpk.fa_id
union 
SELECT ap.nowe_id, ap.inspire_id, ap.wersja_od, ap.wersja_do, ap.wersja_od, ap.uzyt_od, ap.uzyt_do, null, 
zpk.nowe_id ,ap.nowe_id
from migracja.crfop_akt_prawny_gotowy ap
inner join migracja.crfop_zespol_przyr_kraj_gotowy zpk on ap.fop_id = zpk.fop_id
where ap.wersja_do = '9999-12-31 23:59:59.000 +0100' and zpk.wersja_do = '9999-12-31 23:59:59.000 +0100';

delete from imap.fop_obszar_chronionego_krajobrazu_rel_gminy;
insert into imap.fop_obszar_chronionego_krajobrazu_rel_gminy(id, wersja_od, wersja_do, wersjaid, id_fop_obszar_chronionego_krajobrazu, id_slo_gminy)
select nextval('fop__tech_id_seq'), oc.wersja_od, oc.wersja_do, oc.wersja_od, oc.nowe_id, gm.kod_gmi
from migracja.crfop_obszar_chron_krajobrazu_gotowy oc
inner join _gdos_crfopdb_public.fop_prg_gmi gm on oc.fop_id = gm.fop_id
where oc.wersja_do = '9999-12-31 23:59:59.000 +0100';

delete from imap.fop_park_krajobrazowy_rel_gminy;
insert into imap.fop_park_krajobrazowy_rel_gminy(id, wersja_od, wersja_do, wersjaid, id_fop_park_krajobrazowy, id_slo_gminy)
select nextval('fop__tech_id_seq'), pk.wersja_od, pk.wersja_do, pk.wersja_od, pk.nowe_id, gm.kod_gmi
from migracja.crfop_park_krajobrazowy_gotowy pk
inner join _gdos_crfopdb_public.fop_prg_gmi gm on pk.fop_id = gm.fop_id
where pk.wersja_do = '9999-12-31 23:59:59.000 +0100';

delete from imap.fop_park_narodowy_rel_gminy;
insert into imap.fop_park_narodowy_rel_gminy(id, wersja_od, wersja_do, wersjaid, id_fop_park_narodowy, id_slo_gminy)
select nextval('fop__tech_id_seq'), pn.wersja_od, pn.wersja_do, pn.wersja_od, pn.nowe_id, gm.kod_gmi
from migracja.crfop_park_narodowy_gotowy pn
inner join _gdos_crfopdb_public.fop_prg_gmi gm on pn.fop_id = gm.fop_id
where pn.wersja_do = '9999-12-31 23:59:59.000 +0100';

delete from imap.fop_pomnik_przyrody_rel_gminy;
insert into imap.fop_pomnik_przyrody_rel_gminy(id, wersja_od, wersja_do, wersjaid, id_fop_pomnik_przyrody, id_slo_gminy)
select nextval('fop__tech_id_seq'), pp.wersja_od, pp.wersja_do, pp.wersja_od, pp.nowe_id, gm.kod_gmi
from migracja.crfop_pomnik_przyrody_gotowy pp
inner join _gdos_crfopdb_public.fop_prg_gmi gm on pp.fop_id = gm.fop_id
where pp.wersja_do = '9999-12-31 23:59:59.000 +0100';

delete from imap.fop_stanowisko_dokumentacyjne_rel_gminy;
insert into imap.fop_stanowisko_dokumentacyjne_rel_gminy(id, wersja_od, wersja_do, wersjaid, id_fop_stanowisko_dokumentacyjne, id_slo_gminy)
select nextval('fop__tech_id_seq'), sd.wersja_od, sd.wersja_do, sd.wersja_od, sd.nowe_id, gm.kod_gmi
from migracja.crfop_stanowisko_dokumentacyjne_gotowy sd
inner join _gdos_crfopdb_public.fop_prg_gmi gm on sd.fop_id = gm.fop_id
where sd.wersja_do = '9999-12-31 23:59:59.000 +0100';

delete from imap.fop_uzytek_ekologiczny_rel_gminy;
insert into imap.fop_uzytek_ekologiczny_rel_gminy(id, wersja_od, wersja_do, wersjaid, id_fop_uzytek_ekologiczny, id_slo_gminy)
select nextval('fop__tech_id_seq'), ue.wersja_od, ue.wersja_do, ue.wersja_od, ue.nowe_id, gm.kod_gmi
from migracja.crfop_uzytek_ekologiczny_gotowy ue
inner join _gdos_crfopdb_public.fop_prg_gmi gm on ue.fop_id = gm.fop_id
where ue.wersja_do = '9999-12-31 23:59:59.000 +0100';

delete from imap.fop_zespol_przyrodniczo_krajobrazowy_rel_gminy;
insert into imap.fop_zespol_przyrodniczo_krajobrazowy_rel_gminy(id, wersja_od, wersja_do, wersjaid, id_fop_zespol_przyrodniczo_krajobrazowy, id_slo_gminy)
select nextval('fop__tech_id_seq'), zpk.wersja_od, zpk.wersja_do, zpk.wersja_od, zpk.nowe_id, gm.kod_gmi
from migracja.crfop_zespol_przyr_kraj_gotowy zpk
inner join _gdos_crfopdb_public.fop_prg_gmi gm on zpk.fop_id = gm.fop_id
where zpk.wersja_do = '9999-12-31 23:59:59.000 +0100';

DELETE FROM imap.fop_dokument_lacze;
INSERT INTO imap.fop_dokument_lacze (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, uzyt_do, id_fop_dokument, lacze)
SELECT nextval('fop__tech_id_seq'), b.aph_id, a.wersja_od, a.wersja_do , a.wersja_od, a.uzyt_od, a.uzyt_do, a.nowe_id, b.link
FROM migracja.crfop_akt_prawny_gotowy a
join _gdos_crfopdb_public.akt_prawny_hiperlacze b on a.ap_id = b.ap_id
where a.wersja_do = '9999-12-31 23:59:59.000 +0100';
---------------------------------------------------------------------------------------------------
--tabele ogólne nsystem
DELETE FROM imap.rep_pliki;
INSERT INTO imap.rep_pliki(plik_nazwa, plik, id_tabeli, id_obiektu)
select c.filename, c.file, 53, d.nowe_id from _gdos_crfopdb_public.twor_przyrody a join _gdos_crfopdb_public.pomnik_przyrody b on a.pp_id = b.pp_id
join _gdos_crfopdb_public.twor_foto c on a.tp_id = c.twor_przyrody_id join migracja.crfop_twor_przyrody_gotowy d on
d.fop_id = b.fop_id
where wersja_do = '9999-12-31 23:59:59.000 +0100' and miniatura is not true;
--długie czekanie

DO $$
DECLARE
    my_offset INT := 0;
    batch_size INT := 100;
BEGIN
    LOOP
        -- Wstawianie kolejnej partii rekordów
        INSERT INTO imap.rep_pliki (id_dok, plik_nazwa, plik, id_tabeli, id_obiektu)
        SELECT a.nowe_id, b.filename, b.file, 48, a.nowe_id
        FROM migracja.crfop_akt_prawny_gotowy a
        JOIN (select * from _gdos_crfopdb_public.akt_prawny_plik LIMIT batch_size OFFSET my_offset)b ON a.ap_id = b.ap_id
        WHERE a.wersja_do = '9999-12-31 23:59:59.000 +0100'
        --TYLKO NA DEV!!!  LIMIT 100;
        
        -- Sprawdzanie czy wstawiono jakieś rekordy
        IF NOT FOUND THEN
            EXIT;
        END IF;

        -- Zwiększenie offsetu
        my_offset := my_offset + batch_size;
    END LOOP;
END $$;

---------------------------------------------------------------------------------------------------
--kontrola antydublowa
select * from (
SELECT count(id) liczba_dubli, 'fop_rel_fop_lokalizacja' tabela
FROM imap.fop_rel_fop_lokalizacja
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id) liczba_dubli, 'fop_dokument' tabela
FROM imap.fop_dokument
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id) liczba_dubli, 'fop_obszar_chronionego_krajobrazu' tabela
FROM imap.fop_obszar_chronionego_krajobrazu
where wersja_do > '9999-12-31 22:59:59.000'
group by id)
where liczba_dubli >1
union
select * from (
SELECT count(id) liczba_dubli, 'fop_park_krajobrazowy' tabela
FROM imap.fop_park_krajobrazowy
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_park_krajobrazowy_otulina' tabela
FROM imap.fop_park_krajobrazowy_otulina
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_park_narodowy' tabela
FROM imap.fop_park_narodowy
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_park_narodowy_obszar_ochrony' tabela
FROM imap.fop_park_narodowy_obszar_ochrony
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id) liczba_dubli, 'fop_park_narodowy_otulina' tabela
FROM imap.fop_park_narodowy_otulina
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id) liczba_dubli, 'fop_pomnik_przyrody' tabela
FROM imap.fop_pomnik_przyrody
--where wersja_do > '9999-12-31 22:59:59.000'
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id) liczba_dubli, 'fop_pomnik_przyrody_twor' tabela
FROM imap.fop_pomnik_przyrody_twor
--where wersja_do > '9999-12-31 22:59:59.000'
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id) liczba_dubli, 'fop_stanowisko_dokumentacyjne' tabela
FROM imap.fop_stanowisko_dokumentacyjne
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id) liczba_dubli, 'fop_uzytek_ekologiczny' tabela
FROM imap.fop_uzytek_ekologiczny
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id) liczba_dubli, 'fop_zespol_przyrodniczo_krajobrazowy' tabela
FROM imap.fop_zespol_przyrodniczo_krajobrazowy
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_obszar_chronionego_krajobrazu_rel_dokument' tabela
FROM fop_obszar_chronionego_krajobrazu_rel_dokument
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_park_krajobrazowy_rel_dokument' tabela
FROM fop_park_krajobrazowy_rel_dokument
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_park_narodowy_rel_dokument' tabela
FROM fop_park_narodowy_rel_dokument
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_pomnik_przyrody_rel_dokument' tabela
FROM fop_pomnik_przyrody_rel_dokument
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_stanowisko_dokumentacyjne_rel_dokument' tabela
FROM fop_stanowisko_dokumentacyjne_rel_dokument
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_uzytek_ekologiczny_rel_dokument' tabela
FROM fop_uzytek_ekologiczny_rel_dokument
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'fop_zespol_przyrodniczo_krajobrazowy_rel_dokument' tabela
FROM fop_zespol_przyrodniczo_krajobrazowy_rel_dokument
group by id, wersja_od, wersja_do)
where liczba_dubli >1
union
select * from (
SELECT count(nowe_id)  liczba_dubli, 'crfop_konwencja_gotowy' tabela
FROM migracja.crfop_konwencja_gotowy
where wersja_do > '9999-12-31 22:59:59.000'
group by nowe_id)
where liczba_dubli >1
union
select * from (
SELECT count(id)  liczba_dubli, 'rep_pliki' tabela
FROM rep_pliki
group by id)
where liczba_dubli >1