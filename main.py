#!/usr/bin/env python
# coding=utf8

import DataBlock_pb2
import GDS_data_service
import struct
from _datetime import date, timedelta

#数据目录
directory = "ECMWF_HR/TMP/850"
output_directory = "D:/Data/"
# filter = "*.024"
#初始化GDS客户端
service = GDS_data_service.GDSDataService("10.203.7.71", 8080)
#获得指定目录下的所有文件
status ,response = service.getFileList(directory)
# status, response = service.getLatestDataName(directory, filter)
MappingResult = DataBlock_pb2.MapResult()
#如果返回状态为200(Success)
if status == 200:
    if MappingResult is not None:
        #Protobuf的解析
        MappingResult.ParseFromString(response)
        results = MappingResult.resultMap
        #遍历指定目录
        for name_size_pair in results.items():
            #文件名
            fileName = name_size_pair[0]
            today = date.today().strftime("%y%m%d" + '08')
            yesterday = (date.today() + timedelta(days=-1)).strftime("%y%m%d" + '20')
            fileNameDate = fileName[0:8]
            if (yesterday == fileNameDate) or (today == fileNameDate):
            #http请求
                status, response = byteArrayResult = service.getData(directory, fileName)
                ByteArrayResult = DataBlock_pb2.ByteArrayResult()
                if status == 200:
                    ByteArrayResult.ParseFromString(response)
                    if ByteArrayResult is not None:
                        byteArray = ByteArrayResult.byteArray
                        print(len(byteArray))
                        discriminator =struct.unpack("4s",byteArray[:4])[0].decode("gb2312")
                        t = struct.unpack("h",byteArray[4:6])
                        mName = struct.unpack("20s",byteArray[6:26])[0].decode("gb2312")
                        eleName = struct.unpack("50s",byteArray[26:76])[0].decode("gb2312")
                        description = struct.unpack("30s",byteArray[76:106])[0].decode("gb2312")
                        level,y,m,d,h,timezone,period = struct.unpack("fiiiiii",byteArray[106:134])
                        startLon,endLon,lonInterval,lonGridCount = struct.unpack("fffi",byteArray[134:150])
                        startLat,endLat,latInterval,latGridCount = struct.unpack("fffi",byteArray[150:166])
                        isolineStartValue,isolineEndValue,isolineInterval =struct.unpack("fff",byteArray[166:178])
                        gridCount = lonGridCount*latGridCount
                        description =mName.rstrip('\x00')+'_'+eleName.rstrip('\x00')+"_"+str(level)+'('+description.rstrip('\x00')+')'+":"+str(period)
                        if (gridCount == (len(byteArray)-278)/4):
                            #保存MICAPS4类数据
                            with open (output_directory+fileName,'w') as writer:
                                eachline = "diamond 4 "+description
                                writer.write(eachline+"\n")
                                eachline = str(y)+"\t"+str(m)+"\t"+str(d)+"\t"+str(h)+"\t"+str(period)+"\t"+str(level)+"\t"\
                                +str(lonInterval)+"\t"+str(latInterval)+"\t"+str(round(startLon,2))+"\t"\
                                +str(endLon)+"\t"+str(round(startLat,2))+"\t"+str(round(endLat,2))+\
                                "\t"+str(lonGridCount)+"\t"+str(latGridCount)+"\t"+\
                                str(isolineInterval)+"\t"+str(isolineStartValue)+"\t"+\
                                str(isolineEndValue)+"    3    0"
                                writer.write(eachline+"\n")
                                for i in range(gridCount):
                                    if (i != 0 and i % 10 == 0):
                                        writer.write('\n')
                                    gridValue = struct.unpack("f",byteArray[278+i*4:282+i*4])[0]
                                    writer.write(str(round(gridValue,2)).ljust(10))