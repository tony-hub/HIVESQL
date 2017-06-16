create table service_security.dis_time
(
order_id bigint COMMENT'订单ID ',
driver_id bigint COMMENT'司机ID',
passenger_id bigint COMMENT'乘客ID',
distance_rate double COMMENT'实际行驶距离/预估路面距离',
time_rate double COMMENT'实际行驶时间/预估行驶时间'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/dis_time';


Create table dis_time_15 like service_security.dis_time
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/dis_time_15';


 #!/bin/bash
start=$1
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`
hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.dis_time
PARTITION(year,month,day)
select
order_id,
driver_id,
passenger_id,
round(cast(start_dest_distance/1000.0 as double)/normal_distance ,4)as distance_rate,
round(cast( (unix_timestamp(finish_time)-unix_timestamp(begin_charge_time) )/60 as double)/forecast_time,4) as time_rate,
'$years' as year,
'months' as month,
'days' as day
from gulfstream_dw.dw_v_order_base --订单基础表
where concat_ws('-',year,month,day)='$start' and finish_time!='0000-00-00 00:00:00' and begin_charge_time !='0000-00-00 00:00:00'
and normal_distance!=0
distribute by (year,month,day);"
"
select
order_id,
driver_id,
passenger_id,
start_dest_distance/1000 as pre_dis,
normal_distance,
forecast_time,
cast( (unix_timestamp(finish_time)-unix_timestamp(begin_charge_time) )/60 as double) as time_rate
from major_complaint
where concat_ws('-',year,month,day)='2017-05-24' and finish_time!='0000-00-00 00:00:00' and begin_charge_time !='0000-00-00 00:00:00'
and normal_distance!=0 and forecast_time !=0 and order_id=17592838361203;
distribute by (year,month,day);"
"
select
order_id,
driver_id,
passenger_id,
round(normal_distance/cast(start_dest_distance/1000.0 as double),4)as distance_rate,
round(cast( (unix_timestamp(finish_time)-unix_timestamp(begin_charge_time) )/60 as double)/forecast_time,4) as time_rate
from major_complaint_info_com
where concat_ws('-',year,month,day)='2017-05-24' and finish_time!='0000-00-00 00:00:00' and begin_charge_time !='0000-00-00 00:00:00'
and start_dest_distance!=0 ;



17592846628192	565028637246238	654951976961	0.9329	1.5604

select normal_distance from gulfstream_ods.g_order_result and order_id=17592838361203
17592838361203	567950076601744	5000219797157	181515.6	0.0054	2017	05	24
select starting_name,dest_name,order_id,driver_id,passenger_id,start_dest_distance,normal_distance,forecast_time,finish_time,begin_charge_time from gulfstream_dw.dw_v_order_base
 where concat(year,month,day)='20170524' and order_id=17592838034440

17592838309936	567950054913372	661567963136	63401.1	0.0434	2017	05	24
17592838340320	565371855575437	2373085307397	60590.8667	0.025	2017	05	24
17592838656992	567949966689714	143261112	59201.9	0.1238	2017	05	24
17592858885793	565158175711327	1655847659619	50463.4	0.0053	2017	05	24
17592838419775	567950116755566	933744680961	48858.8	0.0125	2017	05	24
17592857989355	567950146644798	2909735425550	43722.9	0.0048	2017	05	24
17592858206645	567950090798361	5000178937132	29446.2	0.0474	2017	05	24
17592844539038	565766220218627	2071756872202	27854.8	0.0314	2017	05	24
17592855199752	564617630057319	2024174914268	23207.31	1.0E-4	2017	05	24
17592856945997	567950154315504	2620626047440	22584.0	0.0022	2017	05	24
17592847750786	566355228363270	2917703293431	19706.34	0.001	2017	05	24
17592850361515	564846606951465	933190500352	19252.5	0.089	2017	05	24
17592847447751	564443034625140	3524418611098	17775.3	6.0E-4	2017	05	24
17592838212231	563498579533825	5000146884781	17015.6	0.019	2017	05	24
17592854594984	567950170476069	5000217048646	16415.3	9.0E-4	2017	05	24
17592850990310	564333918166766	104231479	15814.1	0.0185	2017	05	24
17592838365718	565274472088862	2529294027421	14836.92	0.0423	2017	05	24
17592853230070	567950140392681	2506725660414	13712.1	0.0109	2017	05	24
17592843715358	566043987617665	5000178767028	13288.9	0.0106	2017	05	24
17592846409203	565187178468956	5000122535497	13167.3	0.0025	2017	05	24
17592859017274	565251867345595	5000086940537	13046.0	0.023	2017	05	24
17592858948775	566467877213750	110975794	12554.3	0.0085	2017	05	24
17592855701573	566143438431060	415528259586	12372.8	0.011	2017	05	24
17592854372600	563612148105217	109134440	12282.0	0.023	2017	05	24
17592857612743	566180006602477	5000225300010	11850.2	0.0554	2017	05	24
17592853042064	567950167863562	5000199867292	11668.0	0.0232	2017	05	24
17592846402880	565049016127051	2360584839492	11636.0	0.0212	2017	05	24
17592855119599	565609607992757	3187721967254	11529.38	0.0108	2017	05	24
17592851361829	565984184510869	686780780544	11125.6	0.0182	2017	05	24
17592839424750	566185252357379	1269901361152	11053.0	0.0022	2017	05	24
17592858846309	564439078803908	5000069169204	10719.12	0.0088	2017	05	24
17592849113682	567950032188194	2959579679636	10484.2	0.0333	2017	05	24
17592847775579	567950090487416	5000044968515	10206.0	0.0364	2017	05	24
17592846510011	567950178221940	570951401473	10025.7769	0.0018	2017	05	24
17592845243733	567950113711733	2382880056760	9827.6917	0.0015	2017	05	24
17592843212260	564809655787998	143473002	9783.5	0.0401	2017	05	24
17592851442927	566233307688111	5000160103656	9687.4	0.0076	2017	05	24
17592841987029	567950002065742	638724476929	9383.6	0.0378	2017	05	24
17592842703607	566117376010856	1055713468420	9374.6	0.0418	2017	05	24
17592854851704	567950139614146	1277947150336	9310.2	0.0581	2017	05	24
17592837837745	567950111213132	1990092138667	9140.1	0.0212	2017	05	24
17592837972435	567949992226142	135247826944	8918.1	0.064	2017	05	24
17592845429871	565564269801017	5000224691857	8888.6	0.0238	2017	05	24
17592837807875	566227950113894	135247826944	8883.6	0.013	2017	05	24
17592854382600	567949963977962	2994676695769	8857.5	0.0183	2017	05	24
17592839609319	565902973534625	2773711262287	8546.7	0.0229	2017	05	24
17592847339125	565638601116744	1854485764235	8468.5	0.0097	2017	05	24
17592848393238	565382328296259	3315670918939	8383.4	0.0094	2017	05	24
17592848278422	563934658306048	2438156854692	8196.9	0.0029	2017	05	24
17592855455711	565189990490717	981645860864	8180.1	0.02	2017	05	24
17592841484431	567950167949832	1249019305984	8031.6	0.0154	2017	05	24
17592852021505	567950127447366	5000105793571	7976.5	0.0473	2017	05	24
17592853844438	566186576382469	2664059901629	7909.0	0.021	2017	05	24
17592845948871	564879618219752	2517862714164	7870.0	0.0091	2017	05	24
17592838061950	567950117260373	2863033296888	7859.3885	9.0E-4	2017	05	24
17592846415918	565886937670266	4154301	7809.5	0.0083	2017	05	24
17592857560015	565371994575895	2251468578755	7686.7	0.0061	2017	05	24
15123081758	567950176819809	5000207771485	7675.9	0.0138	2017	05	24
17592838018185	566117706567053	8412458	7589.7	0.0135	2017	05	24
17592858590286	565189486849821	1448643203121	7561.7733	0.0247	2017	05	24
17592844308683	566447145944579	5000135251006	7543.5	0.0234	2017	05	24
17592855680713	566125696850962	2269423413713	7524.1	0.0204	2017	05	24
17592847965901	567950049566118	5000143489482	7485.4	0.0109	2017	05	24
17592853272435	567950146716775	1169118855170	7479.0	0.0582	2017	05	24
17592847233094	564398906221769	2971603775050	7438.5	0.0282	2017	05	24
17592843094362	567950098912708	141954621	7374.0	0.0126	2017	05	24
17592848999882	567950165846120	2567824613826	7246.05	0.0665	2017	05	24
17592843500792	567949956562781	5000170682165	7244.4	0.0056	2017	05	24
17592852954879	565769045016769	2764844437810	7200.6	0.0389	2017	05	24
17592857604586	567950173514252	2119983436545	7198.1	0.0237	2017	05	24
17592846370781	564924496877948	15902880	7188.1	0.0083	2017	05	24
17592843523801	566310450696918	23416634	7148.6	0.0134	2017	05	24
17592853585193	566198732729262	5000077981219	7087.8	0.0051	2017	05	24
17592842227085	567950145238651	2564129493990	7057.6	0.0577	2017	05	24
17592856796495	567950124999719	5000225333360	7043.8	0.0074	2017	05	24
17592842146072	564843442537930	113021561	7016.7	0.0133	2017	05	24
17592850793366	565551644285114	3099535548352	6925.9	0.0347	2017	05	24
17592845088432	564129770049538	109704063	6649.0	0.0278	2017	05	24
17592844011208	565858597148706	758950076417	6631.7	0.0246	2017	05	24
17592856940814	567950067521200	2268348165254	6620.1	0.0359	2017	05	24
17592850237467	567950091302706	2705641647763	6613.4	0.098	2017	05	24
17592843408113	565853791979423	2001072040344	6521.1	0.0309	2017	05	24
17592847971044	566503529456935	5000173406775	6479.0	0.0038	2017	05	24
17592841107790	563558096109569	102427039	6435.7	0.0068	2017	05	24
17592848827792	567950141982552	1697071963210	6417.4	0.0045	2017	05	24
17592855844894	564798295509933	5000225157550	6370.0	0.0608	2017	05	24
17592838040112	565856781607984	5000102626015	6167.15	0.0116	2017	05	24
17592842940820	567950158099846	3151430166595	6089.2	0.0023	2017	05	24
17592849028275	566419454170306	5000225009877	6070.6	0.0451	2017	05	24
17592837640901	566301108996608	5000196948827	6068.2	0.0034	2017	05	24
17592853801371	565892133301323	2387834112480	6053.5	0.0217	2017	05	24
17592838046312	567950143735996	3414569790152	6037.6	0.0043	2017	05	24
17592847285143	567950101436385	5000097538360	5990.1	0.0141	2017	05	24
17592857171309	566396170279103	2999672964488	5987.3	0.0163	2017	05	24
17592847047773	567950154980487	2404748569438	5985.4	0.0245	2017	05	24
17592855906502	566174419912899	3040807	5958.6	0.0208	2017	05	24
15123386894	564122342924288	1692883945546	5898.35	0.0042	2017	05	24
17592858363064	566072501212563	2433686439601	5895.0	0.0155	2017	05	24
17592839425695	566068844563194	3119374804042	5812.7	0.0333	2017	05	24

13895465 \* $percent / 100
2017-05-24—按距离排序	正常行驶距离/预估距离
1%
5.00%				1.4072
10.00%				1.2296
15.00%				1.1498

1% 138954
5% 694773
10% 1389547
15% 2084320

from(
select order_id,driver_id,passenger_id,distance_rate,time_rate from service_security.dis_time
where concat_ws('-',year,month,day)='2017-05-24' and time_rate is not null and distance_rate
is not null order by time_rate desc,distance_rate  desc limit 2084320
)a
select *  order by  time_rate limit 10


2017-05-24—按距离排序	正常行驶距离/预估距离
1.00%	2.2608
5.00%	1.4072
10.00%	1.2296
15.00%	1.1498

按时间排序	正常行驶时间/预估时间
1.00%	3.0758
5.00%	1.85
10.00%	1.5583
15.00%	1.4208
