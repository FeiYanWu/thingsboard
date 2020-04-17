package org.thingsboard.server.dao.sqlts.ts;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.thingsboard.server.dao.model.sqlts.ts.TsKvLatestEntity;
import org.thingsboard.server.dao.sqlts.AbstractLatestInsertRepository;
import org.thingsboard.server.dao.util.MySqlDao;
import org.thingsboard.server.dao.util.SqlTsDao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@SqlTsDao
@MySqlDao
@Repository
@Transactional
public class MySqlLatestInsertRepository extends AbstractLatestInsertRepository {

    private static final String TS_KV_LATEST_CONSTRAINT = "(entity_type, entity_id, key)";

    private static final String INSERT_OR_UPDATE_BOOL_STATEMENT = getInsertOrUpdateStringPsql(TS_KV_LATEST_TABLE, TS_KV_LATEST_CONSTRAINT, BOOL_V, PSQL_ON_BOOL_VALUE_UPDATE_SET_NULLS);
    private static final String INSERT_OR_UPDATE_STR_STATEMENT = getInsertOrUpdateStringPsql(TS_KV_LATEST_TABLE, TS_KV_LATEST_CONSTRAINT, STR_V, PSQL_ON_STR_VALUE_UPDATE_SET_NULLS);
    private static final String INSERT_OR_UPDATE_LONG_STATEMENT = getInsertOrUpdateStringPsql(TS_KV_LATEST_TABLE, TS_KV_LATEST_CONSTRAINT, LONG_V, PSQL_ON_LONG_VALUE_UPDATE_SET_NULLS);
    private static final String INSERT_OR_UPDATE_DBL_STATEMENT = getInsertOrUpdateStringPsql(TS_KV_LATEST_TABLE, TS_KV_LATEST_CONSTRAINT, DBL_V, PSQL_ON_DBL_VALUE_UPDATE_SET_NULLS);
    
    
    private static final String BATCH_UPDATE =
            "UPDATE ts_kv_latest SET ts = ?, bool_v = ?, str_v = ?, long_v = ?, dbl_v = ? WHERE entity_type = ? AND entity_id = ? and `key` = ?";


    private static final String INSERT_OR_UPDATE =
            "INSERT INTO ts_kv_latest (entity_type, entity_id, `key`, ts, bool_v, str_v, long_v, dbl_v) VALUES(?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON duplicate key update ts = ?, bool_v = ?, str_v = ?, long_v = ?, dbl_v = ?;";


    @Override
    public void saveOrUpdate(TsKvLatestEntity entity) {
        processSaveOrUpdate(entity, INSERT_OR_UPDATE_BOOL_STATEMENT, INSERT_OR_UPDATE_STR_STATEMENT, INSERT_OR_UPDATE_LONG_STATEMENT, INSERT_OR_UPDATE_DBL_STATEMENT);
    }

    @Override
    protected void saveOrUpdateBoolean(TsKvLatestEntity entity, String query) {
        entityManager.createNativeQuery(query)
                .setParameter("entity_type", entity.getEntityType().name())
                .setParameter("entity_id", entity.getEntityId())
                .setParameter("key", entity.getKey())
                .setParameter("ts", entity.getTs())
                .setParameter("bool_v", entity.getBooleanValue())
                .executeUpdate();
    }

    @Override
    protected void saveOrUpdateString(TsKvLatestEntity entity, String query) {
        entityManager.createNativeQuery(query)
                .setParameter("entity_type", entity.getEntityType().name())
                .setParameter("entity_id", entity.getEntityId())
                .setParameter("key", entity.getKey())
                .setParameter("ts", entity.getTs())
                .setParameter("str_v", replaceNullChars(entity.getStrValue()))
                .executeUpdate();
    }

    @Override
    protected void saveOrUpdateLong(TsKvLatestEntity entity, String query) {
        entityManager.createNativeQuery(query)
                .setParameter("entity_type", entity.getEntityType().name())
                .setParameter("entity_id", entity.getEntityId())
                .setParameter("key", entity.getKey())
                .setParameter("ts", entity.getTs())
                .setParameter("long_v", entity.getLongValue())
                .executeUpdate();
    }

    @Override
    protected void saveOrUpdateDouble(TsKvLatestEntity entity, String query) {
        entityManager.createNativeQuery(query)
                .setParameter("entity_type", entity.getEntityType().name())
                .setParameter("entity_id", entity.getEntityId())
                .setParameter("key", entity.getKey())
                .setParameter("ts", entity.getTs())
                .setParameter("dbl_v", entity.getDoubleValue())
                .executeUpdate();
    }

    @Override
    public void saveOrUpdate(List<TsKvLatestEntity> entities) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                int[] result = jdbcTemplate.batchUpdate(BATCH_UPDATE, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        TsKvLatestEntity tsKvLatestEntity = entities.get(i);
                        ps.setLong(1, tsKvLatestEntity.getTs());

                        if (tsKvLatestEntity.getBooleanValue() != null) {
                            ps.setBoolean(2, tsKvLatestEntity.getBooleanValue());
                        } else {
                            ps.setNull(2, Types.BOOLEAN);
                        }

                        ps.setString(3, replaceNullChars(tsKvLatestEntity.getStrValue()));

                        if (tsKvLatestEntity.getLongValue() != null) {
                            ps.setLong(4, tsKvLatestEntity.getLongValue());
                        } else {
                            ps.setNull(4, Types.BIGINT);
                        }

                        if (tsKvLatestEntity.getDoubleValue() != null) {
                            ps.setDouble(5, tsKvLatestEntity.getDoubleValue());
                        } else {
                            ps.setNull(5, Types.DOUBLE);
                        }

                        ps.setString(6, tsKvLatestEntity.getEntityType().name());
                        ps.setString(7, tsKvLatestEntity.getEntityId());
                        ps.setString(8, tsKvLatestEntity.getKey());
                    }

                    @Override
                    public int getBatchSize() {
                        return entities.size();
                    }
                });

                int updatedCount = 0;
                for (int i = 0; i < result.length; i++) {
                    if (result[i] == 0) {
                        updatedCount++;
                    }
                }

                List<TsKvLatestEntity> insertEntities = new ArrayList<>(updatedCount);
                for (int i = 0; i < result.length; i++) {
                    if (result[i] == 0) {
                        insertEntities.add(entities.get(i));
                    }
                }

                jdbcTemplate.batchUpdate(INSERT_OR_UPDATE, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        TsKvLatestEntity tsKvLatestEntity = insertEntities.get(i);
                        ps.setString(1, tsKvLatestEntity.getEntityType().name());
                        ps.setString(2, tsKvLatestEntity.getEntityId());
                        ps.setString(3, tsKvLatestEntity.getKey());
                        ps.setLong(4, tsKvLatestEntity.getTs());
                        ps.setLong(9, tsKvLatestEntity.getTs());

                        if (tsKvLatestEntity.getBooleanValue() != null) {
                            ps.setBoolean(5, tsKvLatestEntity.getBooleanValue());
                            ps.setBoolean(10, tsKvLatestEntity.getBooleanValue());
                        } else {
                            ps.setNull(5, Types.BOOLEAN);
                            ps.setNull(10, Types.BOOLEAN);
                        }

                        ps.setString(6, replaceNullChars(tsKvLatestEntity.getStrValue()));
                        ps.setString(11, replaceNullChars(tsKvLatestEntity.getStrValue()));


                        if (tsKvLatestEntity.getLongValue() != null) {
                            ps.setLong(7, tsKvLatestEntity.getLongValue());
                            ps.setLong(12, tsKvLatestEntity.getLongValue());
                        } else {
                            ps.setNull(7, Types.BIGINT);
                            ps.setNull(12, Types.BIGINT);
                        }

                        if (tsKvLatestEntity.getDoubleValue() != null) {
                            ps.setDouble(8, tsKvLatestEntity.getDoubleValue());
                            ps.setDouble(13, tsKvLatestEntity.getDoubleValue());
                        } else {
                            ps.setNull(8, Types.DOUBLE);
                            ps.setNull(13, Types.DOUBLE);
                        }
                    }

                    @Override
                    public int getBatchSize() {
                        return insertEntities.size();
                    }
                });
            }
        });
    }

//    @Override
//    public void saveOrUpdate(List<TsKvLatestEntity> entities) {
//
//        jdbcTemplate.batchUpdate(INSERT_OR_UPDATE, new BatchPreparedStatementSetter() {
//            @Override
//            public void setValues(PreparedStatement ps, int i) throws SQLException {
//                ps.setString(1, entities.get(i).getEntityType().name());
//                ps.setString(2, entities.get(i).getEntityId());
//                ps.setString(3, entities.get(i).getKey());
//                ps.setLong(4, entities.get(i).getTs());
//                ps.setLong(9, entities.get(i).getTs());
//
//                if (entities.get(i).getBooleanValue() != null) {
//                    ps.setBoolean(5, entities.get(i).getBooleanValue());
//                    ps.setBoolean(10, entities.get(i).getBooleanValue());
//                } else {
//                    ps.setNull(5, Types.BOOLEAN);
//                    ps.setNull(10, Types.BOOLEAN);
//                }
//
//                ps.setString(6, replaceNullChars(entities.get(i).getStrValue()));
//                ps.setString(11, replaceNullChars(entities.get(i).getStrValue()));
//
//
//                if (entities.get(i).getLongValue() != null) {
//                    ps.setLong(7, entities.get(i).getLongValue());
//                    ps.setLong(12, entities.get(i).getLongValue());
//                } else {
//                    ps.setNull(7, Types.BIGINT);
//                    ps.setNull(12, Types.BIGINT);
//                }
//
//                if (entities.get(i).getDoubleValue() != null) {
//                    ps.setDouble(8, entities.get(i).getDoubleValue());
//                    ps.setDouble(13, entities.get(i).getDoubleValue());
//                } else {
//                    ps.setNull(8, Types.DOUBLE);
//                    ps.setNull(13, Types.DOUBLE);
//                }
//            }
//
//            @Override
//            public int getBatchSize() {
//                return entities.size();
//            }
//        });
//    }
}
