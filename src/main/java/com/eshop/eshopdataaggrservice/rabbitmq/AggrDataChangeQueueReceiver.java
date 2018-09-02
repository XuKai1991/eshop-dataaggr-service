package com.eshop.eshopdataaggrservice.rabbitmq;

import com.alibaba.fastjson.JSONObject;
import com.eshop.eshopdataaggrservice.cons.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
@RabbitListener(queues = "aggr-data-change-queue")
@Slf4j
public class AggrDataChangeQueueReceiver {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @RabbitHandler
    public void process(String content) {
        log.info("聚合商品同步服务消费消息 : " + content);
        JSONObject jsonObject = JSONObject.parseObject(content);
        // dim_type
        String dimType = jsonObject.getString(Constants.RQ_DIM_TYPE);
        if ("brand".equals(dimType)) {
            processBrandDimDataChange(jsonObject);
        } else if ("category".equals(dimType)) {
            processCategoryDimDataChange(jsonObject);
        } else if ("product".equals(dimType)) {
            processProductDimDataChange(jsonObject);
        } else if ("product_intro".equals(dimType)) {
            processProductIntroDimDataChange(jsonObject);
        }
    }

    private void processBrandDimDataChange(JSONObject jsonObject) {
        Long id = jsonObject.getLong(Constants.RQ_ID);
        // 多此一举，看一下，查出来一个品牌数据，然后直接就原样写redis
        // 实际上是这样子的，我们这里是简化了数据结构和业务，实际上任何一个维度数据都不可能只有一个原子数据
        // 品牌数据，肯定是结构多变的，结构比较复杂，有很多不同的表，不同的原子数据
        // 实际上这里肯定是要将一个品牌对应的多个原子数据都从redis查询出来，然后聚合之后写入redis
        String jsonBrand = redisTemplate.opsForValue().get(Constants.REDIS_BRAND_KEY + id);
        // 将关于brand的所有原子数据从redis中查出来并拼装，拼装后写入redis
        if (StringUtils.isNotEmpty(jsonBrand)) {
            redisTemplate.opsForValue().set(Constants.REDIS_DIM_BRAND_KEY + id, jsonBrand);
        } else {
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_DIM_BRAND_KEY + id);
        }
    }

    private void processCategoryDimDataChange(JSONObject jsonObject) {
        Long id = jsonObject.getLong(Constants.RQ_ID);
        String jsonCategory = redisTemplate.opsForValue().get(Constants.REDIS_CATEGORY_KEY + id);
        // 将关于category的所有原子数据从redis中查出来并拼装，拼装后写入redis
        if (StringUtils.isNotEmpty(jsonCategory)) {
            redisTemplate.opsForValue().set(Constants.REDIS_DIM_CATEGORY_KEY + id, jsonCategory);
        } else {
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_DIM_CATEGORY_KEY + id);
        }
    }

    private void processProductIntroDimDataChange(JSONObject jsonObject) {
        Long productId = jsonObject.getLong(Constants.RQ_ID);
        String jsonProductIntro = redisTemplate.opsForValue().get(Constants.REDIS_PRODUCT_INTRO_KEY + productId);
        // 将关于category的所有原子数据从redis中查出来并拼装，拼装后写入redis
        if (StringUtils.isNotEmpty(jsonProductIntro)) {
            redisTemplate.opsForValue().set(Constants.REDIS_DIM_PRODUCT_INTRO_KEY + productId, jsonProductIntro);
        } else {
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_DIM_PRODUCT_INTRO_KEY + productId);
        }
    }

    private void processProductDimDataChange(JSONObject jsonObject) {
        Long productId = jsonObject.getLong(Constants.RQ_ID);
        String productJsonStr = redisTemplate.opsForValue().get(Constants.REDIS_PRODUCT_KEY + productId);
        if (StringUtils.isNotEmpty(productJsonStr)) {
            JSONObject productJsonObject = JSONObject.parseObject(productJsonStr);
            // 使用redis的mget批量获取数据
            List<String> jsonResult = redisTemplate.opsForValue().multiGet(
                    Arrays.asList(
                            Constants.REDIS_PRODUCT_PROPERTY_KEY + productId,
                            Constants.REDIS_PRODUCT_SPECIFICATION_KEY + productId
                    )
            );
            String productPropertyJsonStr = jsonResult.get(0);
            String productSpecificationJsonStr = jsonResult.get(1);
            // 根据productId从redis获取productProperty，如果非空就拼装到product中
            // String productPropertyJsonStr = redisTemplate.opsForValue().get(Constants.REDIS_PRODUCT_PROPERTY_KEY + productId);
            if (StringUtils.isNotEmpty(productPropertyJsonStr)) {
                productJsonObject.put("product_property", JSONObject.parseObject(productPropertyJsonStr));
            }
            // 根据productId从redis获取productSpecification，如果非空就拼装到product中
            // String productSpecificationJsonStr = redisTemplate.opsForValue().get(Constants.REDIS_PRODUCT_SPECIFICATION_KEY + productId);
            if (StringUtils.isNotEmpty(productSpecificationJsonStr)) {
                productJsonObject.put("product_specification", JSONObject.parseObject(productSpecificationJsonStr));
            }
            redisTemplate.opsForValue().set(Constants.REDIS_DIM_PRODUCT_KEY + productId, productJsonObject.toJSONString());
        } else {
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_DIM_PRODUCT_KEY + productId);
        }
    }
}
