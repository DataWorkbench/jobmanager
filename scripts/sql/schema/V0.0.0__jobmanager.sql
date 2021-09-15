-- DATABASE
CREATE DATABASE IF NOT EXISTS data_workbench;
USE data_workbench;

create table jobmanager (
	jobid varchar(24),
	spaceid varchar(24),
	noteid varchar(9),
	status varchar(20),
	message text,
	paragraph varchar(2000),
	createtime timestamp default now(),
	updatetime timestamp,
	resources varchar(2000),
	zeppelinserver varchar(256)
);

alter table jobmanager add constraint jobmanager_pkey primary key(jobid);
create index jobmanager_spaceid_idx on jobmanager(spaceid);
