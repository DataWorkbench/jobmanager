
create table job_manager (
	job_id varchar(24),
	space_id varchar(24),
	note_id varchar(9),
	status int,
	message text,
	paragraph varchar(2000),
	created BIGINT(20) UNSIGNED NOT NULL,
	updated BIGINT(20) UNSIGNED NOT NULL,
	resources varchar(2000),
	zeppelin_server varchar(256)
);

alter table job_manager add constraint job_manager_pkey primary key(job_id);
create index job_manager_spaceid on job_manager(space_id);
