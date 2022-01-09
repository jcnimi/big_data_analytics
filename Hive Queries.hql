--create the database
CREATE SCHEMA IF NOT EXISTS big_data_anlytics;

--create the table inside the new database
CREATE TABLE IF NOT EXISTS  big_data_anlytics.UNSWNB15(
srcip string, sport int, dstip string, dsport int, proto string,
state string, dur Float, sbytes int, dbytes int, sttl int, dttl int,
sloss int, dloss int, service string, Sload Float, Dload Float,
Spkts int, Dpkts int, swin int, dwin int, stcpb int, dtcpb int,
smeansz	int, dmeansz int, trans_depth int, res_bdy_len int, Sjit Float,
Djit Float, Stime BIGINT, Ltime BIGINT, Sintpkt Float, Dintpkt Float,
tcprtt Float, synack Float, ackdat Float, is_sm_ips_ports int, ct_state_ttl int,
ct_flw_http_mthd int, is_ftp_login int, ct_ftp_cmd int, ct_srv_src int, ct_srv_dst int,
ct_dst_ltm int, ct_src_ltm int, ct_src_dport_ltm int, ct_dst_sport_ltm int, 
ct_dst_src_ltm int, attack_cat string, Label int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--load the data from the csv file into the table
load data local inpath '/home/cloudera/Desktop/data' into table big_data_anlytics.UNSWNB15;

--show the data to confirm that the load was successful
select * from big_data_anlytics.UNSWNB15 limit 5;

-- counting the number of connection by protocol
select proto as Protocol , count(*) as Connection
from big_data_anlytics.UNSWNB15
group by proto
order by Connection desc;

-- attacks by protocole
with temp as (
    select proto as Protocol , 
    sum(case WHEN label = 0 then 1 else 0 end) as Normal,
    sum(case WHEN label = 1 then 1 else 0 end) as Attack,
    (sum(case WHEN label = 1 then 1 else 0 end) / count(*) ) * 100 as Percentage
    from big_data_anlytics.UNSWNB15
    group by proto
    order by Attack desc, Normal  desc
)
select Protocol, 
    format_number(Normal, 0) Normal,
    format_number(Attack, 0) Attack,
    format_number(Percentage, 2) Percentage
from temp;

--check attacks by ip and destination port
with temp as (
    select srcip, dsport, count(*) as Attacks
    from big_data_anlytics.UNSWNB15
    WHERE label = 1
    GROUP BY srcip, dsport
)

describe big_data_anlytics.UNSWNB15;