package org.thingsboard.server.dao.sql.component;

import org.springframework.stereotype.Repository;
import org.thingsboard.server.common.data.UUIDConverter;
import org.thingsboard.server.dao.model.sql.ComponentDescriptorEntity;
import org.thingsboard.server.dao.util.MySqlDao;
import org.thingsboard.server.dao.util.SqlDao;

@SqlDao
@MySqlDao
@Repository
public class MySqlComponentDescriptorInsertRepository extends AbstractComponentDescriptorInsertRepository{
    private static final String ID = "id = :id";
    private static final String CLAZZ_CLAZZ = "clazz = :clazz";

    private static final String P_KEY_CONFLICT_STATEMENT = "(id)";
    private static final String UNQ_KEY_CONFLICT_STATEMENT = "(clazz)";

    private static final String ON_P_KEY_CONFLICT_UPDATE_STATEMENT = getUpdateStatement(CLAZZ_CLAZZ);
    private static final String ON_UNQ_KEY_CONFLICT_UPDATE_STATEMENT = getUpdateStatement(ID);

    private static final String INSERT_OR_UPDATE_ON_P_KEY_CONFLICT = getInsertOrUpdateStatement(P_KEY_CONFLICT_STATEMENT, ON_P_KEY_CONFLICT_UPDATE_STATEMENT);
    private static final String INSERT_OR_UPDATE_ON_UNQ_KEY_CONFLICT = getInsertOrUpdateStatement(UNQ_KEY_CONFLICT_STATEMENT, ON_UNQ_KEY_CONFLICT_UPDATE_STATEMENT);


    @Override
    protected ComponentDescriptorEntity doProcessSaveOrUpdate(ComponentDescriptorEntity entity, String query) {
        getQuery(entity, query).executeUpdate();
        return entityManager.find(ComponentDescriptorEntity.class, UUIDConverter.fromTimeUUID(entity.getId()));
    }

    @Override
    public ComponentDescriptorEntity saveOrUpdate(ComponentDescriptorEntity entity) {
        return saveAndGet(entity, INSERT_OR_UPDATE_ON_P_KEY_CONFLICT, INSERT_OR_UPDATE_ON_UNQ_KEY_CONFLICT);
    }

    private static String getInsertOrUpdateStatement(String conflictKeyStatement, String updateKeyStatement) {
        return "INSERT INTO component_descriptor (id, actions, clazz, configuration_descriptor, name, scope, search_text, type) VALUES (:id, :actions, :clazz, :configuration_descriptor, :name, :scope, :search_text, :type) on duplicate key update " + updateKeyStatement;
    }

    private static String getUpdateStatement(String id) {
        return "actions = :actions, " + id + ", configuration_descriptor = :configuration_descriptor, name = :name, scope = :scope, search_text = :search_text, type = :type";
    }
}