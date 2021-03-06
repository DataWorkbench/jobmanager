-- DATABASE
CREATE DATABASE IF NOT EXISTS data_workbench;
USE data_workbench;

create table jobmanager (
	id varchar(24),
	spaceid varchar(24),
	noteID varchar(9),
	status varchar(20),
	message text,
	paragraph varchar(2000),
	createtime timestamp default now(),
	updatetime timestamp,
	resources varchar(2000)
);

alter table jobmanager add constraint jobmanager_pkey primary key(id);
-- create index job_status on jobmanager(id, status);
alter table jobmanager add CONSTRAINT jobmanager_chk_status check(status = "failed" or status = "finish" or status = "running");
create index jobmanager_spaceid_idx on jobmanager(spaceid);

