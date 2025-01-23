

drop table if exists staging2.article;
drop table if exists staging2.journal;
drop table if exists staging2.field_ford;
drop table if exists staging2.field_of_study;
drop table if exists staging2.institution;
go

drop schema if exists staging2;
go

create schema staging2;
go

create table staging2.field_of_study (
 sid int primary key,
 name varchar(50)
);

insert into staging2.field_of_study values (1, 'Natural sciences');
insert into staging2.field_of_study values (2, 'Engineering and Technology');
insert into staging2.field_of_study values (3, 'Medical and Health Sciences');
insert into staging2.field_of_study values (4, 'Agricultural and veterinary sciences');
insert into staging2.field_of_study values (5, 'Social Sciences');
insert into staging2.field_of_study values (6, 'Humanities and Arts');
go



create table staging2.field_ford (
	fid int primary key,
	sid int foreign key references staging2.field_of_study(sid),
	name varchar(100)
)
go

create table staging2.journal (
  aid int primary key identity,
  year int,
  name varchar(300),
  issn varchar(10),
  eissn varchar(10),
  article_count int,
  zone varchar(6),
  czech_or_slovak varchar(4),
  fid int foreign key references staging2.field_ford(fid)
);
go


create table staging2.article (
  year int,
  UT_WoS varchar(25),
  name varchar(8000),
  type varchar(40),
  journal_name varchar(300),
  issn varchar(10),
  eissn varchar(10),
  aid int references staging2.journal(aid),
  fid int foreign key references staging2.field_ford(fid),
  authors varchar(8000),
  VO_coresponding_author varchar(8000),
  author_count int,
  czech_or_slovak varchar(4),
  VO varchar(4000),
  institution_count int,
  zone varchar(6)
);
go

create table staging2.institution (
  iid int primary key identity,
  name varchar(1000),
  ico int,
  street varchar(500),
  psc int,
  town varchar(200),
  legal_form varchar(500),
  main_goal text, 
  created datetime2
)

