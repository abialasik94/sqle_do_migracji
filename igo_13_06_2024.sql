drop table if exists migracja.igo_gatunek;
create table migracja.igo_gatunek as 
select nextval('igo__tech_id_seq') objectid_from_sequence, lsid lokalnyid, '3-10-2008'::date wersja_od, '9999-12-31 23:59:59.000'::timestamptz wersja_do, '3-10-2008'::date wersjaid, a.nazwalacinska nazwalacinska, nazwapolska nazwapolska, 
easinid ideasin, eunomenguid idnomen, urldoopisu opislink, null::int id_slo_igo_gatunek_rodzaj, 
case when a.rozprzestrzenionynaszerokaskale is true then 'T' when a.rozprzestrzenionynaszerokaskale is false then 'N' end as szerokaskala,
case when a.wymagaszybkiejeliminacji is true then 'T' when a.wymagaszybkiejeliminacji is false then 'N' end as szybkaeliminacja,
null zwierzekregowe, a.id stare_id
from _gdos_iasdb_public.gatunek a join _gdos_iasdb_public.gatunektezaurus b on a.id = b.id
join _gdos_iasdb_public.easincode c on b.easinid = c.speciesid;

delete from imap.igo_gatunek;
insert into imap.igo_gatunek (id, lokalnyid, wersja_od, wersja_do, wersjaid, nazwalacinska, nazwapolska, ideasin, idnomen, opislink, id_slo_igo_gatunek_rodzaj, czy_szerokaskala,
czy_szybkaeliminacja, czy_zwierzekregowe)
select objectid_from_sequence, lokalnyid, wersja_od, wersja_do, wersjaid, nazwalacinska, nazwapolska, ideasin, idnomen, opislink, id_slo_igo_gatunek_rodzaj, szerokaskala,
szybkaeliminacja, zwierzekregowe
from migracja.igo_gatunek;

delete from imap.igo_podmiot;

insert into imap.igo_podmiot (lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, id_slo_igo_podmiot_rodzaj, imie, nazwisko, stanowisko, nazwa, adr_miejscowosc,
adr_ulica, adr_nr_budynku, adr_kod_pocztowy, informacjedodatkowe)
select a.id, '3-10-2008', '9999-12-31 23:59:59.000'::timestamptz, '3-10-2008', nazwauzytkownika, 
b.id, LEFT(a.imieinazwisko, POSITION(' ' IN a.imieinazwisko) - 1) AS imie,
    SUBSTRING(a.imieinazwisko FROM POSITION(' ' IN a.imieinazwisko) + 1) AS nazwisko, null, nazwapodmiotu, adresmiasto, 
LEFT(a.adresulica, LENGTH(a.adresulica) - POSITION(' ' IN REVERSE(a.adresulica))) AS ulica,
    RIGHT(a.adresulica, POSITION(' ' IN REVERSE(a.adresulica)) - 1) AS nr, adreskodpocztowy, uwagi
from _gdos_iasdb_public.przetrzymywaniegatunku a left join _gdos_iasdb_public.miejsceprzetrzymywania b on a.id_miejsceprzetrzymywania = b.id
left join _gdos_iasdb_public.uzytkownik c on a.id_uzytkownikrejestrujacy = c.id;

insert into imap.igo_podmiot(nazwa, wersja_od, wersja_do, wersjaid)
select distinct zwalczajacy, '3-10-2008'::timestamp, '9999-12-31 23:59:59.000'::timestamptz, '3-10-2008'::timestamp
from _gdos_iasdb_public.wystepowaniegatunku
where zwalczajacy not in ('', 'Brak danych') and zwalczajacy is not null;

drop table if exists migracja.igo_wystepowanie;
create table migracja.igo_wystepowanie as 
select nextval('igo__tech_id_seq') nowe_id, a.id stare_id_wystepowanie, a.id lokalnyid, '3-10-2008'::date wersja_od, '9999-12-31 23:59:59.000'::timestamptz wersja_do, 
'3-10-2008'::date wersjaid, nazwauzytkownika uzyt_od, b.objectid_from_sequence igo_gatunek_objectid,
datastwierdzenia data_stwierdzenia, datarejestracji data_rejestracji, liczebnoscmin liczebnoscpoczatkowa_min, liczebnoscmax liczebnoscpoczatkowa_max, 
id_jednostkaliczebnosci id_slo_igo_liczebnosc_jednostka, id_podgrupa id_slo_igo_grupa_danych, zrodlodanych zrodlodanych, uwagi uwagi, 
opismiejscawystepowania miejscestwierdzeniaopis,
case when st_geometrytype(a.geom) in ('ST_Point', 'ST_MultiPoint') then a.geom else null end as geometria_pkt,
case when st_geometrytype(a.geom) in ('ST_LineString', 'ST_MultiLineString') then a.geom else null end as geometria_lin,
case when st_geometrytype(a.geom) in ('ST_Polygon', 'ST_MultiPolygon') then a.geom else null end as geometria_pow
from _gdos_iasdb_public.wystepowaniegatunku a join migracja.igo_gatunek b on a.id_gatunek = b.stare_id
left join _gdos_iasdb_public.uzytkownik c on a.id_uzytkownikrejestrujacy = c.id;

delete from imap.igo_wystepowanie;
insert into imap.igo_wystepowanie (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, id_igo_gatunek, data_stwierdzenia, data_rejestracji, 
liczebnoscpoczatkowa_min, liczebnoscpoczatkowa_max, id_slo_igo_liczebnosc_jednostka, id_slo_igo_grupa_danych, zrodlodanych, uwagi,
miejscestwierdzeniaopis, geometria_pkt, geometria_lin, geometria_pow)
select nowe_id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, igo_gatunek_objectid, data_stwierdzenia, data_rejestracji, 
liczebnoscpoczatkowa_min, liczebnoscpoczatkowa_max, id_slo_igo_liczebnosc_jednostka, id_slo_igo_grupa_danych, zrodlodanych, uwagi,
miejscestwierdzeniaopis,  geometria_pkt, geometria_lin, geometria_pow
from migracja.igo_wystepowanie;

delete from imap.igo_wystepowanie_rel_gminy;
insert into imap.igo_wystepowanie_rel_gminy (wersja_od, wersja_do, id_igo_wystepowanie, id_slo_gminy)
select '3-10-2008', '9999-12-31 23:59:59.000'::timestamptz, a.nowe_id, vagg.kodjednostki from (
select st_centroid(wyst.geometria_pow) centroid_wyst, wyst.nowe_id nowe_id
from migracja.igo_wystepowanie wyst) a
join ref_prg.vm_a03_granice_gmin vagg on ST_Within(a.centroid_wyst, vagg.geom) ;

delete from imap.igo_wystepowanie_dzialania;
insert into imap.igo_wystepowanie_dzialania (lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, id_igo_wystepowanie, data_rejestracji, liczebnoscaktualna_min,
liczebnoscaktualna_max, id_slo_igo_liczebnosc_jednostka, id_slo_igo_dzialania_status, id_slo_igo_dzialania_metoda, wplywinnegatunki, id_igo_podmiot,
kosztdzialanzaradczych, skutecznosc, srodkirenaturyzacyjne, kosztsrodkowrenaturyzacyjnych, uwagi, opismiejscadzialan, id_slo_igo_grupa_danych, geometria_pkt, 
geometria_lin, geometria_pow)
select a.id, '3-10-2008', '9999-12-31 23:59:59.000'::timestamptz, '3-10-2008', e.nazwauzytkownika, c.nowe_id, datarejestracji, liczebnoscmin, liczebnoscmax, id_jednostkaliczebnosci,
id_statuszwalczania, id_metodazwalczania, wplywnainnegatunki, ip.id id_podmiotu, koszt, opisskutecznosci, opisrenaturyzacji, kosztrenaturyzacji, a.uwagi,
opismiejscazwalczania, id_podgrupa,
case when st_geometrytype(a.geom) in ('ST_Point', 'ST_MultiPoint') then a.geom else null end as geometria_pkt,
case when st_geometrytype(a.geom) in ('ST_LineString', 'ST_MultiLineString') then a.geom else null end as geometria_lin,
case when st_geometrytype(a.geom) in ('ST_Polygon', 'ST_MultiPolygon') then a.geom else null end as geometria_pow
from _gdos_iasdb_public.wystepowaniegatunku a join migracja.igo_wystepowanie c on c.stare_id_wystepowanie = a.id
left join _gdos_iasdb_public.uzytkownik e on a.id_uzytkownikrejestrujacy = e.id
left join (select id, nazwa from imap.igo_podmiot where nazwa not in ('', 'Brak danych') and nazwa is not null) ip on ip.nazwa = a.zwalczajacy


drop table if exists migracja.igo_zezwolenie;
create table migracja.igo_zezwolenie as 

select nextval('igo__tech_id_seq') nowe_id, a.id stare_id_przetrzymywanie, a.id lokalnyid, '3-10-2008'::timestamp wersja_od, 
'9999-12-31 23:59:59.000'::timestamptz wersja_do, '3-10-2008'::timestamp wersjaid, e.nazwauzytkownika uzyt_od, ip.id podmiot_id,
a.numerpozwolenia numerpozwolenia, a.datarejestracji::timestamp data_rejestracji, a.datawydaniapozwolenia::timestamp data_wydania,
a.datazawieszeniapozwoleniaod::timestamp data_zawieszenia_od, a.datazawieszeniapozwoleniado::timestamp data_zawieszenia_do,
a.datacofnieciapozwolenia::timestamp data_cofniecia, 
case when a.kontrolapozwolenia is true then 'T' when a.kontrolapozwolenia is false then 'N' end as kontrolapozwolenia, 
a.uwagi uwagi, a.id_organwydajacy

from _gdos_iasdb_public.przetrzymywaniegatunku a 
left join (select id, lokalnyid from igo_podmiot) ip on ip.lokalnyid::int = a.id
left join _gdos_iasdb_public.uzytkownik e on a.id_uzytkownikrejestrujacy = e.id;

delete from imap.igo_zezwolenie;
insert into imap.igo_zezwolenie (id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, id_igo_podmiot, numerzezwolenia, 
data_rejestracji, data_wydania, data_zawieszenia_od, data_zawieszenia_do, data_cofniecia, czy_kontrolazezwolenia, uwagi, 
id_slo_igo_zezwolenie_organ)
select nowe_id, lokalnyid, wersja_od, wersja_do, wersjaid, uzyt_od, podmiot_id, numerpozwolenia, data_rejestracji, data_wydania, 
data_zawieszenia_od, data_zawieszenia_do, data_cofniecia, kontrolapozwolenia, uwagi, id_organwydajacy
from migracja.igo_zezwolenie;

drop table if exists migracja.igo_zezwolenie_gatunek;
create table migracja.igo_zezwolenie_gatunek as 
select nextval('igo__tech_id_seq') nowe_id, a.id stare_id_opis_przet, a.id lokalnyid, '3-10-2008'::timestamp wersja_od, 
'9999-12-31 23:59:59.000'::timestamptz wersja_do, '3-10-2008'::timestamp wersjaid, b.nowe_id::int igo_zezwolenie_id, c.nowe_id::int igo_gatunek_id,
a.id_celczynnosci::int, a.id_nomenklaturascalona::int, a.id_jednostkaliczebnosci, a.liczebnoscmin::int, a.liczebnoscmax::int, a.oznaczenie,
a.microchip
from _gdos_iasdb_public.opisprzetrzymywaniagatunku a 
left join (select nowe_id, stare_id_przetrzymywanie from migracja.igo_zezwolenie) b on b.stare_id_przetrzymywanie = a.id_przetrzymywaniegatunku
left join (select objectid_from_sequence nowe_id, stare_id from migracja.igo_gatunek) c on c.stare_id = a.id_gatunek;

insert into imap.igo_zezwolenie_gatunek(id, lokalnyid, wersja_od, wersja_do, wersjaid, id_igo_zezwolenie, id_igo_gatunek, 
id_slo_igo_przetrzymywany_gatunek_czynnosc_cel, id_slo_igo_przetrzymywany_gatunek_nomenklatura, inneoznakowanie, mikrochip)
select nowe_id, lokalnyid, wersja_od, wersja_do, wersjaid, igo_zezwolenie_id, igo_gatunek_id,
id_celczynnosci, id_nomenklaturascalona, oznaczenie, microchip
from migracja.igo_zezwolenie_gatunek;

delete from imap.igo_zezwolenie_gatunek_czynnosc;
insert into imap.igo_zezwolenie_gatunek_czynnosc(lokalnyid, wersja_od, wersja_do, wersjaid, id_igo_zezwolenie_gatunek, id_slo_igo_przetrzymywany_gatunek_czynnosc,
data_od, data_do, adr_miejscowosc, adr_ulica, adr_nr_budynku, adr_nr_lokalu, adr_kod_pocztowy, id_slo_igo_liczebnosc_jednostka,
liczebnosc_min, liczebnosc_max)
select a.id, '3-10-2008', '9999-12-31 23:59:59.000'::timestamptz, '3-10-2008', b.nowe_id, a.id_czynnosc, a.czynnoscod, a.czynnoscdo, 
TRIM(concat(SPLIT_PART(SPLIT_PART(miejsce, ', ', 2), ' ', 2),' ', SPLIT_PART(SPLIT_PART(miejsce, ', ', 2), ' ', 3))) AS miejscowosc,
TRIM(concat(SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(miejsce, ', ', 1), 'ul. ', ''), 'os. ', ''), ' ', -3), ' ', SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(miejsce, ', ', 1), 'ul. ', ''), 'os. ', ''), ' ', -2))) as ulica_nazwa,
SPLIT_PART(SPLIT_PART(SPLIT_PART(miejsce, ', ', 1), ' ', -1), '/', 1) AS ulica_nr,
SPLIT_PART(SPLIT_PART(SPLIT_PART(miejsce, ', ', 1), ' ', -1), '/', 2) AS lokal_nr,
SPLIT_PART(SPLIT_PART(miejsce, ', ', 2), ' ', 1) AS kod_pocztowy, id_jednostkaliczebnosci, liczebnoscmin, liczebnoscmax
from _gdos_iasdb_public.opisczynnosciprzetrzymywanegogatunku a
left join migracja.igo_zezwolenie_gatunek b on a.id_opisprzetrzymywanegogatunku = b.stare_id_opis_przet;

INSERT INTO imap.rep_pliki(plik_nazwa, id_tabeli, id_obiektu)
select a.nazwapliku, 118, b.nowe_id 
from _gdos_iasdb_public.plikdokumentu a 
join migracja.igo_zezwolenie b on a.id_przetrzymywaniegatunku = b.stare_id_przetrzymywanie;