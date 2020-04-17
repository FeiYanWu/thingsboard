package org.thingsboard.rule.engine.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.rule.engine.rabbitmq.TbRabbitMqNodeConfiguration;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "redis",
        configClazz = TbRedisNodeConfigureation.class,
        nodeDescription = "Publish messages to the Redis",
        nodeDetails = "Will publish message payload to Redis.",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbActionNodeRedisConfig",
        iconUrl = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbDpzcGFjZT0icHJlc2VydmUiIHZlcnNpb249IjEuMSIgeT0iMHB4IiB4PSIwcHgiIHZpZXdCb3g9IjAgMCAxMDAwIDEwMDAiPjxwYXRoIHN0cm9rZS13aWR0aD0iLjg0OTU2IiBkPSJtNTE0LjQwMDYwOCw4My40MjQ4MDdjMTEwLjQwNTk1NSwwIDE5OS45NzM1ODYsODYuMjg1NjEyIDE5OS45NzM1ODYsMTkyLjU0MzY2YzAsMTA2LjI1ODA5OSAtODkuNTY3NjMxLDE5Mi40NjA2ODUgLTE5OS45NzM1ODYsMTkyLjQ2MDY4NWMtMTEwLjQwNjAwOCwwIC0xOTkuOTczNjQsODYuMjg1NjkgLTE5OS45NzM2NCwxOTIuNTQzNzEyYzAsMTA2LjI1ODA3MyA4OS41Njc2MzEsMTkyLjQ2MDY4NSAxOTkuOTczNjQsMTkyLjQ2MDY4NWMyMjAuODExOTA5LDAgNDAwLjAzMzM4NSwtMTcyLjQ4ODMyNyA0MDAuMDMzMzg1LC0zODUuMDA0NDIyYzAsLTIxMi41MTYxMjEgLTE3OS4yMjE0NzYsLTM4NS4wMDQzNDUgLTQwMC4wMzMzODUsLTM4NS4wMDQzNDVsMCwwLjAwMDAyNnptMCwxMzIuMzczNzcyYy0zNC41MTk2MTgsMCAtNjIuNTE4NzQzLDI2Ljk0NzEwNiAtNjIuNTE4NzQzLDYwLjE2OTg4N2MwLDMzLjIyMjgzMyAyNy45OTkwNDQsNjAuMTY5ODg3IDYyLjUxODc0Myw2MC4xNjk4ODdjMzQuNTE5NjcyLDAgNjIuNTE4NjYyLC0yNi45NDcxMDYgNjIuNTE4NjYyLC02MC4xNjk4ODdjMCwtMzMuMjIyNzgxIC0yNy45OTg5OTEsLTYwLjE2OTg4NyAtNjIuNTE4NjYyLC02MC4xNjk4ODd6bTAsMzg1LjAwNDQyMmMzNC41MDE4MDQsMCA2Mi41MTg2NjIsMjYuOTY0MjI1IDYyLjUxODY2Miw2MC4xNjk4ODdjMCwzMy4yMDU1ODUgLTI4LjAxNjgzMiw2MC4xNjk5MzkgLTYyLjUxODY2Miw2MC4xNjk5MzljLTM0LjUwMTg1NywwIC02Mi41MTg3NDMsLTI2Ljk2NDMyOCAtNjIuNTE4NzQzLC02MC4xNjk5MzljMCwtMzMuMjA1NTg1IDI4LjAxNjg4NSwtNjAuMTY5ODg3IDYyLjUxODc0MywtNjAuMTY5ODg3em0zOTEuMDYwOTEsLTEzMi4xMjg1MzJjMCwyMTIuMzU4MjEyIC0xNzguODcwMzY5LDM4NC41MDg1NzIgLTM5OS41MTgzNjcsMzg0LjUwODU3MmMtMjIwLjY0Nzg5LDAgLTM5OS41MTgzNjcsLTE3Mi4xNTAzNTkgLTM5OS41MTgzNjcsLTM4NC41MDg1NzJjMCwtMjEyLjM1ODI5IDE3OC44NzA0NzcsLTM4NC41MDg3MDEgMzk5LjUxODM2NywtMzg0LjUwODcwMWMyMjAuNjQ3OTk4LDAgMzk5LjUxODM2NywxNzIuMTUwNDExIDM5OS41MTgzNjcsMzg0LjUwODcwMXoiLz48L3N2Zz4="
)
public class TbRedisNode implements TbNode {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final String ERROR = "error";

    private RedisTemplate<String, String> redisTemplate;
    private TbRedisNodeConfigureation config;
    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbRedisNodeConfigureation.class);

        redisTemplate = new RedisTemplate<String, String>();
        RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration();
        redisStandaloneConfiguration.setHostName(this.config.getHost());
        redisStandaloneConfiguration.setPort(this.config.getPort());

        redisTemplate.setConnectionFactory(new JedisConnectionFactory(redisStandaloneConfiguration));
        redisTemplate.setKeySerializer(new StringRedisSerializer());//key的序列化适配器
        redisTemplate.setValueSerializer(new StringRedisSerializer());//value的序列化适配器，也可以自己编写，大部分场景StringRedisSerializer足以满足需求了。
        redisTemplate.afterPropertiesSet();

    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        System.out.println(msg);
        redisTemplate.opsForValue().set(msg.getOriginator().getId().toString(), msg.getData());
    }

    @Override
    public void destroy() {

    }
}
