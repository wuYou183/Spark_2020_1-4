# 原在jupyter上运行，因此进行分块


# 第一部分：转换文件
import pandas as pd

data=pd.read_csv('/home/hadoop/covid_19_data.csv')
with open('/home/hadoop/covid_19_data.txt','a+',encoding='utf-8') as f:
	for line in data.valies:
		f.write((str(line[1])+'\t'+str(line[2])+'\t'+str(line[3])+'\t'
			+str(line[4])+'\t'+str(line[5])+'\t'+str(line[6])+'\t'+str(line[7])+'\n'))


# 第二部分：Spark处理
import json
import pyspark.sql.functions as func
from pyspark import SparkConf,SparkContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

# 统一日期形式
def toData(inputStr):
	newStr=''
	s1=inputStr[0:2]
	s2=inputStr[3:5]
	s3=inputStr[6:]
	newStr=s3+'-'+s1+'-'s2
	date=datetime.strptime(newStr,'%Y-%m-%d')
	return data

# 主程序
spark=SparkSession.builder.config(conf=SparkConf()).getOrCreate()
fields=[StructField('Date',DateType(),False),StructField('Province',StringType(),False),
	StructField('Country',StringType(),False),StructField('Confirmed',IntegerType(),False),
	StructField('Deaths',IntegerType(),False),StructField('Recovered',IntegerType(),False)]
schema=StructType(fields)

rdd0=spark.sparkContext.textFile('/user/hadoop/covid_19_data.txt')
rdd1=rdd0.map(lambda x:x.split('\t')).map(lambda p:Row(toData(p[0]),p[1],p[2],int(p[3]),int(p[4]),int(p[5])))

shemaUsInfo=spark.createDataFrame(rdd1,schema)
shemaUsInfo.createOrReplaceTempView('info')  # 注册为临时表

# 1、计算每日累计确诊病例数、死亡数、痊愈数
df=shemaUsInfo.groupBy('Date').agg(func.sum('Confirmed'),func.sum('Deaths'),func.sum('Recovered')).sort(shemaUsInfo['Date'].asc())
# （1）重命名：列
df1=df.withColumnRenamed('sum(Confirmed)','Confirmed').withColumnRenamed('sum(Deaths)','Deaths').withColumnRenamed('sum(Recovered)','Recovered')
# （2）写入hdfs，存储为json文件
df1.repartition(1).write.json('worldresult1.json')
# （3）注册为临时表供下一步使用
df1.createOrReplaceTempView('total')

# 2、计算每日较前一日新增的确诊病例数、死亡数、死亡率
df2=spark.sql('select t1.Date,t1.Confirmed-t2.Confirmed as ConfirmedIncrease,t1.Deaths-t2.Deaths as deathIncrease from total t1,total t2 where t1.Date=date_add(t2.Date,1)')
df2.sort(df2['Date'].asc()).repartition(1).write.json('worldresult2.json')  # 写入hdfs，存储为json文件

# 3、统计截至4月21日的累计确诊病例数、死亡数、痊愈数
df3=spark.sql('select Date,Country,sum(Confirmed) as totalConfirmed,sum(Deaths) as totalDeaths,round(sum(Deaths)/sum(Confirmed),4) as deathRate,sum(Recovered) as totalRecovered,round(sum(Recovered)/sum(Confirmed),4) as RecoveredRate from Info where Date=to_date('2020-04-21','yyyy-MM-dd') group by Date,Country')
df3.sort(df3["totalConfirmed"].desc()).repartition(1).write.json("worldresult3.json")  # 写入hdfs，存储为json文件
df3.createOrReplaceTempView("eachInfo")  # 注册为临时表

# 4、找出全球确诊病例数最多的10个国家
df4=spark.sql('select Date,Country,totalConfirmed from eachInfo order by totalConfirmed desc limit 10')
df4.repartition(1).write.json('worldresult4.json')  # 写入hdfs，存储为json文件

# 5、找出全球死亡数最多的10个国家
df5=spark.sql('select Date,Country,totalDeaths from eachInfo order by totalDeaths desc limit 10')
df5.repartition(1).write.json('worldresult5.json')  # 写入hdfs，存储为json文件

# 6、找出全球痊愈数最多的10个国家
df6=spark.sql('select Date,Country,totalRecovered from eachInfo order by totalRecovered desc limit 10')
df6.repartition(1).write.json('worldresult6.json')  # 写入hdfs，存储为json文件

# 7、统计截至4月21日的全球病死率
df7=spark.sql('select 1 as sign,Date,"earth" as Country,round(sum(totalDeaths)/sum(totalConfirmed),4) as deathRate from eachInfo group by Date union select 2 as sign,Date,Country,deathRate from eachInfo').cache()
df7.sort(df7['sign'].asc(),df7['deathRate'].desc()).repartition(1).write.json('worldresult7.json')  # 写入hdfs，存储为json文件


# 第三部分：可视化
import json
import pandas as pd
import plotly.offline as py
import plotly.express as px
import plotly.graph_objs as go

# 1、全球疫情分布变化图（地图）
cases=pd.read_csv('/home/hadoop/covid_19_data.csv')
grp=cases.groupby(['Date','Country'])['Confirmed','Deaths','Recovered'].max()
grp=grp.reset_index()
grp['Date']=pd.to_datetime(grp['Date'])
grp['Date']=grp['Date'].dt.strftime('%m/%d/%Y')
grp['Active']=grp['Confirmed']-grp['Recovered']-grp['Deaths']
grp['Country']=grp['Country']
fig=px.choropleth(grp,locations='Country',locationmode='country names',
	color='Confirmed',hover_name='Country',hover_data=[grp.Recovered,grp,Deaths,grp.Active],
	projection='natural earth',animation_frame='Date',width=1000,height=700,
	color_continuous_scale='Reds',range_color=[1000,50000],title='World Map of Coronavirus')
fig.show()

# 2、全球确诊病例数、死亡数、痊愈数变化图
data=[]
confirmed=[]
deaths=[]
recovered=[]
with open('/home/hadoop/worldresult/worldresult1','r') as f:
	while True:
		line=f.readline()
		if not line: break
		js=json.loads(line)
		date.append(str(js['Date']))
		confirmed.append(int(js['Confirmed']))
		deaths.append(int(js['Deaths']))
		recovered.append(int('Recovered'))
trace0=go.Scatter(x=date,y=confirmed,name='已确诊人数')
trace1=go.Scatter(x=date,y=deaths,name='死亡人数')
trace2=go.Scatter(x=date,y=recovered,name='痊愈人数')
data=[trace0,trace1,trace2]
fig=go.Figure(data)
fig.update_layout(title="全球已确诊数和死亡数和痊愈数",xaxis_title="时间",yaxis_title="人数")
fig.show()

# 3、全球每日新增确诊病例数、死亡数变化图
date=[]
confirmedIncrease=[]
deathIncrease=[]
with open('/home/hadoop/worldresult/worldresult2','r') as f:
	while True:
		line=f.readline()
		if not line: break
		js=json.loads(line)
		date.append(str(js['Date']))
		confirmedIncrease.append(int(js['ConfirmedIncrease']))
		deathIncrease.append(int(js['deathIncrease']))
trace0=go.Scatter(x=date,y=confirmedIncrease,name='已确诊人数')
trace1=go.Scatter(x=date,y=deathIncrease,name='死亡人数')
data=[trace0,trace1]
fig=go.Figure(data)
fig.update_layout(title="全球每日新增已确诊数和死亡数",xaxis_title="时间",yaxis_title="人数")
fig.show()

# 4、不同国家截至4月21日的确诊病例数、死亡数、痊愈数统计图
Country=[]
totalConfirmed=[]
totalDeaths=[]
totalRecovered=[]
with open('/home/hadoop/worldresult/worldresult3','r') as f:
	while True:
		line=f.readline()
		if not line: break
		js=json.loads(line)
		Country.append(str(js['Country']))
		totalConfirmed.append(int(js['totalConfirmed']))
		totalDeaths.append(int(js['totalDeaths']))
		totalRecovered.append(int(js['totalRecovered']))
trace0=go.Bar(x=Country,y=totalConfirmed,opacity=0.5,name='不同国家的已确诊数')
trace1=go.Scatter(x=Country,y=totalDeaths,mode='lines',name='不同国家的死亡数',yaxis='y2')  # yaxis='y2'表示绘制双y轴图
trace2=go.Scatter(x=Country,y=totalRecovered,mode='lines',name='不同国家的痊愈数',yaxis='y2')
data=[trace0,trace1,trace2]
layout=go.Layout(title="不同国家的已确诊数,死亡数,痊愈数",xaxis=dict(title="国家"),
	yaxis=dict(title="人数"),
	yaxis2=dict(title="死亡数",overlaying="y",side="right"),  # 给第二个y轴，添加标题，指定第二个y轴，在右侧
	yaxis3=dict(title="痊愈数",overlaying="y",side="right"),
	legend=dict(x=0.78,y=0.98,font=dict(size=12,color="black")))
fig=go.Figure(data=data,layout=layout)
fig.show()

# 5、全球确诊病例数、死亡数、痊愈数前10国家梯形图
# （1）全球确诊病例数前10国家梯形图数据
Country1=[]
totalConfirmed=[]
with open('/home/hadoop/worldresult/worldresult4','r') as f:
	while True:
		line=f.readline()
		if not line: break
		js=json.loads(line)
		Country1.append(str(js['Country']))
		totalConfirmed.append(int(js['totalConfirmed']))
# （2）全球死亡数前10国家梯形图数据
Country2=[]
totalDeaths=[]
with open('/home/hadoop/worldresult/worldresult5','r') as f:
	while True:
		line=f.readline()
		if not line: break
		js=json.loads(line)
		Country2.append(str(js['Country']))
		totalDeaths.append(int(js['totalDeaths']))
# （3）全球痊愈数前10国家梯形图数据
Country3=[]
totalRecovered=[]
with open('/home/hadoop/worldresult/worldresult6','r') as f:
	while True:
		line=f.readline()
		if not line: break
		js=json.loads(line)
		Country3.append(str(js['Country']))
		totalRecovered.append(int(js['totalRecovered']))
# （4）绘制梯形图
fig=tools.make_subplots(rows=3,cols=1)
trace0=go.Funnel(y=Country1,x=totalConfirmed) 
trace2=go.Funnel(y=Country2,x=totalDeaths) 
trace3=go.Funnel(y=Country3,x=totalRecovered) 
fig.append_trace(trace0,1,1)
fig.append_trace(trace2,2,1)
fig.append_trace(trace3,3,1)
fig.update_layout(title='全球确诊、死亡、痊愈最多的10个国家',height=900,width=900)
fig.show()

# 6、不同国家截至4月21日的病死率统计图
Country=[]
deathRate=[]
earth=[]
with open('/home/hadoop/worldresult/worldresult7','r') as f:
	while True:
		line=f.readline()
		if not line: break
		js=json.loads(line)
		Country.append(str(js['Country']))
		deathRate.append(float(js['deathRate']))
		earth.append(deathRate[0])
trace0=go.Scatter(x=Country[1:],y=deathRate[1:],name='国家死亡率')
trace1=go.Scatter(x=Country[1:],y=earth,name='全球死亡率')
data=[trace0,trace1]
fig=go.Figure(data)
fig.update_layout(title="全球各国疫情死亡率",xaxis_title="国家",yaxis_title="死亡率")
fig.show()

# 7、全球疫情病死率饼图
Country=[]
deathRate=[]
with open('/home/hadoop/worldresult/worldresult7','r') as f:
	while True:
		line=f.readline()
		if not line: break
		js=json.loads(line)
		Country.append(str(js['Country']))
		deathRate.append(float(js['deathRate']))
pyplt=py.offline.plot
lables=['全球死亡率','存活率']
values=[deathRate[0],1-deathRate[0]]
trace=[go.Pie(lables=lables,values=values)]
layout=go.Layout(title='全球疫情')
fig=go.Figure(data=trace,layout=layout)
fig.show()