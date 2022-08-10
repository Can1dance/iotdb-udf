---
typora-copy-images-to: img
typora-root-url: img

---

## **Range**Pro

### **函数简介**

本函数用于查找时间序列的范围异常等级。将根据提供的等级范围，判断输入数据是否越界，即异常，并输出异常点的值和异常等级。

函数名： RANGEPRO

输入序列： 仅支持单个输入序列，类型为 INT32 / INT64 / FLOAT / DOUBLE

参数：

![img](D:/%E4%B8%AD%E6%A0%B8UDF/udf-test/udf-test/github-res2/img/wps3988.tmp.jpg) 

· abnormal_level:范围异常检测的等级划分。

· 必须以”,”分隔，划分范围从左到右依次增大。

· abnormal_name:范围异常检测的等级昵称。

· 从左到右指定异常等级的名字,以”,”分隔从左到右取名，如果abnormal_level为7个数，那abnormal_name就比如为6个，如上图所示。

abnormal_name 里面有name="normal",的异常等级(不区分大小写),则不输出该区间的数据算该区间为正常数据没有任何异常。

输出序列： 输出单个序列，类型为TEXT。

### **使用示例**

注册UDF

CREATE FUNCTION RANGEPRO AS 'org.apache.iotdb.library.anomaly.UDTFRangePro'

指定abnormal_level和abnormal_name

输入序列：

![img](D:/%E4%B8%AD%E6%A0%B8UDF/udf-test/udf-test/github-res2/img/wps3989.tmp.jpg) 

用于查询的 SQL 语句：

select RANGEPRO(temperature,"abnormal_level"="-9999,-30,-20,100,120,130,150,9999","abnormal_name"="L3,L2,L1,NORMAL,H1,H2,H3") as temperature_abnormal from root.ln.wf01.wt01

![img](D:/%E4%B8%AD%E6%A0%B8UDF/udf-test/udf-test/github-res2/img/wps398A.tmp.jpg) 

### 注册流程

 **一.获得udf**

从官网下载二进制可执行文件

您可以从 [http://gofile.me/6WI2y/cI8ODdA6B](http://iotdb.apache.org/Download/)上下载已经编译好的可执行程序udf-test-1.0-SNAPSHOT-jar-with-dependencies.jar

使用源码编译

您从https://github.com/Can1dance/iotdb-udf.git 仓库获取

打包命令:

```mvn
mvn clean package
```

jar包出现在target目录下 udf-test-1.0-SNAPSHOT-jar-with-dependencies.jar

**二.将udf-test-1.0-SNAPSHOT-jar-with-dependencies.jar放到放置到目录apache-iotdb-0.12.4-all-bin\ext\udf下.**

apache-iotdb-0.12.4-all-bin可以从<http://iotdb.apache.org/Download/>下载

**三.启动IoTDB服务**

windows

```
sbin\start-server.bat
```

linux

```
nohup sbin/start-server.sh >/dev/null 2>&1 &
```

详细参考：https://iotdb.apache.org/zh/UserGuide/Master/QuickStart/QuickStart.html

**四.启动cli客户端**

windows

```
sbin\start-cli.bat
```

linux

```
sbin/start-cli.sh
```



![1660101308328](D:/%E4%B8%AD%E6%A0%B8UDF/udf-test/udf-test/github-res2/img/1660101308328.png)

**五.使用 SQL 语句注册该 UDF.**

```
CREATE FUNCTION RANGEPRO AS 'org.apache.iotdb.udf.UDTFRangePro'
```

![1660101595639](D:/%E4%B8%AD%E6%A0%B8UDF/udf-test/udf-test/github-res2/img/1660101595639.png)

### 数据测试

原始数据

```
select QF_01_1RCP704MP_AVALUE from root.CNNP.QF.01
```

![1660101849725](D:/%E4%B8%AD%E6%A0%B8UDF/udf-test/udf-test/github-res2/img/1660101849725.png)

```
select QF_01_1RCP704MP_AVALUE from root.CNNP.QF.01 where QF_01_1RCP704MP_AVALUE >=5.4
```

![1660102661943](D:/%E4%B8%AD%E6%A0%B8UDF/udf-test/udf-test/github-res2/img/1660102661943.png)

**用RANGEPRO查询**

```
select RANGEPRO(QF_01_1RCP704MP_AVALUE,"abnormal_level"="5.4,5.6,5.9,9999","abnormal_name"="高1,高2,高3") from root.CNNP.QF.01
```

**查询结果**

![1660101919056](/1660101919056.png)