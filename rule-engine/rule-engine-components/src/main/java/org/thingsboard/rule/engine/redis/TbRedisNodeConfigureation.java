package org.thingsboard.rule.engine.redis;


import lombok.Data;
import org.thingsboard.rule.engine.api.NodeConfiguration;
@Data
public class TbRedisNodeConfigureation  implements NodeConfiguration<TbRedisNodeConfigureation> {

    private String host;
    private int port;
    @Override
    public TbRedisNodeConfigureation defaultConfiguration() {
        TbRedisNodeConfigureation configuration = new TbRedisNodeConfigureation();

        configuration.setHost("127.0.0.1");
        configuration.setPort(6379);
        return configuration;
    }
}
