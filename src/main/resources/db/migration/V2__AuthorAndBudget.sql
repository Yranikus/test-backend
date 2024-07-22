update budget
set type = 'Расход'
where type = 'Комиссия';

create table author
(
    id          serial primary key,
    fullName    text      not null,
    dateCreated timestamp not null
);

alter table budget
add authorId int references author