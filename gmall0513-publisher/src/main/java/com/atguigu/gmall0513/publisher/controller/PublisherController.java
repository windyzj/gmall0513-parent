package com.atguigu.gmall0513.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0513.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String dateString){
        Long dauTotal = publisherService.getDauTotal(dateString);

        List<Map>  totalList=new ArrayList<>();
        HashMap dauMap = new HashMap();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        totalList.add(dauMap);


        HashMap midMap = new HashMap();

        midMap.put("id","new_mid");
        midMap.put("name","新增设备");
        midMap.put("value",323);

        totalList.add(midMap);

        return  JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date") String dateString){
        if("dau".equals(id)) {
            Map<String, Long> dauTotalHoursTD = publisherService.getDauTotalHours(dateString);
            String yesterday = getYesterday(dateString);
            Map<String, Long> dauTotalHoursYD = publisherService.getDauTotalHours(yesterday);

            Map hourMap = new HashMap();
            hourMap.put("today", dauTotalHoursTD);
            hourMap.put("yesterday", dauTotalHoursYD);

            return JSON.toJSONString(hourMap);
        }else{
            return  null;
        }
    }

    private String   getYesterday(String today){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date todayD = simpleDateFormat.parse(today);
            Date yesterdayD = DateUtils.addDays(todayD, -1);
            String yesterday = simpleDateFormat.format(yesterdayD);
            return  yesterday;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;

    }

}
