create table if not exists customer
(
    id   serial primary key,
    name text not null
);

create table if not exists customer_job_buckets
(
    customer_id bigint references customer (id),
    bucket      text not null
);