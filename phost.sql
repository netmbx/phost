create table posts (
       id varchar
          primary key
          not null,
       date timestamptz
            not null,
       posted boolean
              default false,
       message text
               not null
);

create index posts_date on posts(date);
