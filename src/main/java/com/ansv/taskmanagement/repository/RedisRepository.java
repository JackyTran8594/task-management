package com.ansv.taskmanagement.repository;

import com.ansv.taskmanagement.dto.redis.AccessToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class RedisRepository {

    private static final Logger logger = LoggerFactory.getLogger(RedisRepository.class);

    private HashOperations hashOperations;

    private RedisTemplate redisTemplate;


    public RedisRepository(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.hashOperations = this.redisTemplate.opsForHash();
    }

    public void saveToken(String jsonToken, String type, String uuid) {
        try {
            hashOperations.put(type, uuid, jsonToken);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    };

    public void updateToken(String jsonToken, String type, String uuid) {
        saveToken(jsonToken, type, uuid);
    };

    public Object getToken(String uuid, String type) {
        return hashOperations.get(type,  uuid);
    };

    public Object getTokenByObject(String uuid, String type) {
        return (Object) hashOperations.get(type,  uuid);
    };

    public void deleteToken(String uuid, String type) {
        hashOperations.delete(type, uuid);
    };

}
