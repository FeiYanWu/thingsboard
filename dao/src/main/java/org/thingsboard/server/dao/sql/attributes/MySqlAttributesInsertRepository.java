package org.thingsboard.server.dao.sql.attributes;

import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.thingsboard.server.dao.model.sql.AttributeKvEntity;
import org.thingsboard.server.dao.util.MySqlDao;
import org.thingsboard.server.dao.util.SqlDao;

import java.sql.Types;
import java.util.List;

@SqlDao
@MySqlDao
@Repository
@Transactional
public class MySqlAttributesInsertRepository extends AttributeKvInsertRepository {
    private static final String ON_BOOL_VALUE_UPDATE_SET_NULLS = "str_v = null, long_v = null, dbl_v = null";
    private static final String ON_STR_VALUE_UPDATE_SET_NULLS = "bool_v = null, long_v = null, dbl_v = null";
    private static final String ON_LONG_VALUE_UPDATE_SET_NULLS = "str_v = null, bool_v = null, dbl_v = null";
    private static final String ON_DBL_VALUE_UPDATE_SET_NULLS = "str_v = null, long_v = null, bool_v = null";

    private static final String INSERT_OR_UPDATE_BOOL_STATEMENT = getInsertOrUpdateString(BOOL_V, ON_BOOL_VALUE_UPDATE_SET_NULLS);
    private static final String INSERT_OR_UPDATE_STR_STATEMENT = getInsertOrUpdateString(STR_V, ON_STR_VALUE_UPDATE_SET_NULLS);
    private static final String INSERT_OR_UPDATE_LONG_STATEMENT = getInsertOrUpdateString(LONG_V , ON_LONG_VALUE_UPDATE_SET_NULLS);
    private static final String INSERT_OR_UPDATE_DBL_STATEMENT = getInsertOrUpdateString(DBL_V, ON_DBL_VALUE_UPDATE_SET_NULLS);
    private static final String INSERT_OR_UPDATE =
            "INSERT INTO attribute_kv (entity_type, entity_id, attribute_type, attribute_key, str_v, long_v, dbl_v, bool_v, last_update_ts) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON duplicate key update str_v = ?, long_v = ?, dbl_v = ?, bool_v = ?, last_update_ts = ?;";
    
    @Override
    public void saveOrUpdate(AttributeKvEntity entity) {
        processSaveOrUpdate(entity, INSERT_OR_UPDATE_BOOL_STATEMENT, INSERT_OR_UPDATE_STR_STATEMENT, INSERT_OR_UPDATE_LONG_STATEMENT, INSERT_OR_UPDATE_DBL_STATEMENT);
    }

    private static String getInsertOrUpdateString(String value, String nullValues) {
        return "INSERT INTO attribute_kv (entity_type, entity_id, attribute_type, attribute_key, " + value + ", last_update_ts) VALUES (:entity_type, :entity_id, :attribute_type, :attribute_key, :" + value + ", :last_update_ts) ON duplicate key update " + value + " = :" + value + ", last_update_ts = :last_update_ts," + nullValues;
    }


    @Override
    protected void saveOrUpdate(List<AttributeKvEntity> entities) {
        entities.forEach(entity -> {
            jdbcTemplate.update(INSERT_OR_UPDATE, ps -> {
                ps.setString(1, entity.getId().getEntityType().name());
                ps.setString(2, entity.getId().getEntityId());
                ps.setString(3, entity.getId().getAttributeType());
                ps.setString(4, entity.getId().getAttributeKey());
                ps.setString(5, entity.getStrValue());
                ps.setString(10, entity.getStrValue());

                if (entity.getLongValue() != null) {
                    ps.setLong(6, entity.getLongValue());
                    ps.setLong(11, entity.getLongValue());
                } else {
                    ps.setNull(6, Types.BIGINT);
                    ps.setNull(11, Types.BIGINT);
                }

                if (entity.getDoubleValue() != null) {
                    ps.setDouble(7, entity.getDoubleValue());
                    ps.setDouble(12, entity.getDoubleValue());
                } else {
                    ps.setNull(7, Types.DOUBLE);
                    ps.setNull(12, Types.DOUBLE);
                }

                if (entity.getBooleanValue() != null) {
                    ps.setBoolean(8, entity.getBooleanValue());
                    ps.setBoolean(13, entity.getBooleanValue());
                } else {
                    ps.setNull(8, Types.BOOLEAN);
                    ps.setNull(13, Types.BOOLEAN);
                }

                ps.setLong(9, entity.getLastUpdateTs());
                ps.setLong(14, entity.getLastUpdateTs());
            });
        });
    }
}
