package com.atguigu.gmall0513.publisher.service.impl;

import com.atguigu.gmall0513.publisher.mapper.DauMapper;
import com.atguigu.gmall0513.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauTotalHours(String date) {
        //变换格式 [{"LH":"11","CT":489},{"LH":"12","CT":123},{"LH":"13","CT":4343}]
        //===》 {"11":383,"12":123,"17":88,"19":200 }
        List<Map> dauListMap = dauMapper.selectDauTotalHours(date);
        Map<String ,Long> dauMap =new HashMap();
        for (Map map : dauListMap) {
            String  lh =(String) map.get("LH");
            Long  ct =(Long) map.get("CT");
            dauMap.put(lh,ct);
        }
        return dauMap;
    }


}
