create table job_manager
(
    instance_id  varchar(24),
    space_id     varchar(24),
    note_id      varchar(50),
    session_id   varchar(50),
    statement_id varchar(50),
    flink_id     varchar(32),
    state        int,
    message      text,
    created      bigint(20) UNSIGNED NOT NULL,
    updated      BIGINT(20) UNSIGNED NOT NULL
);

alter table job_manager
    add constraint job_manager_pkey primary key (instance_id);

