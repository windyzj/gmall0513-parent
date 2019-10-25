package com.atguigu.gmall0513.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {

    public static void main(String[] args) {
 // 1 连接canal的服务端
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop1", 11111), "example", "", "");

  // 2 抓取数据
        while (true){
            canalConnector.connect();
            canalConnector.subscribe("gmall0513.*");
            //一个message=一次抓取  一次抓取可以抓多个sql的执行结果集
            Message message = canalConnector.get(100);

            if(message.getEntries().size()==0){
                System.out.println("没有数据，休息5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
  // 3 抓取数据后，提取数据
                //一个entry 代表一个sql执行的结果集
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //业务数据 StoreValue
                    if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {

                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            //反序列化工具
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        String tableName = entry.getHeader().getTableName();

 // 4 处理业务数据  发送kafka 到对应的topic
                        CanalHandler canalHandler = new CanalHandler(rowChange.getEventType(), tableName, rowDatasList);
                        canalHandler.handle();

                    }

                }

            }

        }
    }
}
