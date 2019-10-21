package com.atguigu.gmall0513.publisher.service;

import java.util.Map;

public interface PublisherService {

    public Long getDauTotal(String date);

    public Map<String,Long> getDauTotalHours(String date);

}
