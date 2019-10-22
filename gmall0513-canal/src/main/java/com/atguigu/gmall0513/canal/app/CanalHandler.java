package com.atguigu.gmall0513.canal.app;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public  void  handle(){
        if(tableName.equals("order_info")&&eventType== CanalEntry.EventType.INSERT){
            //遍历行集
            for (CanalEntry.RowData rowData : rowDataList) {
                //列集
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                for (CanalEntry.Column column : afterColumnsList) {
                    String name = column.getName();
                    String value = column.getValue();
                    System.out.println(  name +"::"+value);

                }

            }

        }


    }


}
