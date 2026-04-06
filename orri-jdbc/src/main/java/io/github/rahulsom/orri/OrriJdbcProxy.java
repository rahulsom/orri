package io.github.rahulsom.orri;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

/**
 * Wraps JDBC objects so metadata reflects Google Sheets semantics.
 */
final class OrriJdbcProxy {
    private OrriJdbcProxy() {}

    static Connection wrap(OrriSession session) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class[] {Connection.class}, new ConnectionHandler(session));
    }

    private record ConnectionHandler(OrriSession session) implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return switch (method.getName()) {
                    case "getMetaData" -> wrapMetaData(session.connection().getMetaData(), session);
                    case "isReadOnly" -> session.readOnly();
                    case "setReadOnly" -> setReadOnly(args);
                    case "createStatement" -> wrapStatement((Statement) method.invoke(session.connection(), args));
                    case "prepareStatement" ->
                        wrapPreparedStatement(
                                (PreparedStatement) method.invoke(session.connection(), args), (String) args[0]);
                    case "unwrap" -> unwrap(args[0]);
                    case "isWrapperFor" -> isWrapperFor(args[0]);
                    default -> method.invoke(session.connection(), args);
                };
            } catch (InvocationTargetException exception) {
                throw exception.getCause();
            }
        }

        private Object setReadOnly(Object[] args) throws SQLException {
            boolean requestedReadOnly = args != null && args.length == 1 && Boolean.TRUE.equals(args[0]);
            if (requestedReadOnly != session.readOnly()) {
                throw new SQLFeatureNotSupportedException(
                        "Orri connections use a fixed readOnly mode chosen at connect time");
            }
            session.connection().setReadOnly(session.readOnly());
            return null;
        }

        private Statement wrapStatement(Statement statement) {
            return (Statement) Proxy.newProxyInstance(
                    Statement.class.getClassLoader(),
                    new Class[] {Statement.class},
                    new StatementHandler(session, statement, null));
        }

        private PreparedStatement wrapPreparedStatement(PreparedStatement statement, String sql) {
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class[] {PreparedStatement.class},
                    new StatementHandler(session, statement, sql));
        }

        private Object unwrap(Object type) throws SQLException {
            if (type instanceof Class<?> targetType && targetType.isInstance(session.connection())) {
                return session.connection();
            }
            return session.connection().unwrap((Class<?>) type);
        }

        private Object isWrapperFor(Object type) throws SQLException {
            if (type instanceof Class<?> targetType && targetType.isInstance(session.connection())) {
                return true;
            }
            return session.connection().isWrapperFor((Class<?>) type);
        }
    }

    private record StatementHandler(OrriSession session, Object delegate, String preparedSql)
            implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String sql = sql(method.getName(), args);
            SqlMutation mutation = sql == null ? null : SqlMutation.parse(sql);
            CreateTableSpec createTable = sql == null ? null : CreateTableSpec.parse(sql);
            CreateFilterViewSpec createFilterView =
                    sql == null ? null : CreateFilterViewSpec.parse(sql, session.snapshot());
            DropRelationSpec dropRelation = sql == null ? null : DropRelationSpec.parse(sql);
            AlterTableSpec alterTable = sql == null ? null : AlterTableSpec.parse(sql);
            AlterViewSpec alterView = sql == null ? null : AlterViewSpec.parse(sql, session.snapshot());
            validate(mutation, createTable, createFilterView, dropRelation, alterTable, alterView, sql);

            if (dropRelation != null) {
                return dropRelation(method.getName(), dropRelation);
            }

            if (alterView != null) {
                return alterView(method.getName(), alterView);
            }

            try {
                Object result = method.invoke(delegate, args);
                synchronize(method.getName(), mutation, createTable, createFilterView, alterTable);
                return result;
            } catch (InvocationTargetException exception) {
                throw exception.getCause();
            }
        }

        private String sql(String methodName, Object[] args) {
            if (preparedSql != null && methodName.startsWith("execute")) {
                return preparedSql;
            }
            if (args != null && args.length > 0 && args[0] instanceof String sql) {
                return sql;
            }
            return null;
        }

        private void validate(
                SqlMutation mutation,
                CreateTableSpec createTable,
                CreateFilterViewSpec createFilterView,
                DropRelationSpec dropRelation,
                AlterTableSpec alterTable,
                AlterViewSpec alterView,
                String sql)
                throws SQLException {
            boolean schemaOrDataMutation = mutation != null
                    || createTable != null
                    || createFilterView != null
                    || dropRelation != null
                    || alterTable != null
                    || alterView != null;
            if (!schemaOrDataMutation) {
                return;
            }

            if (session.readOnly()) {
                throw new SQLFeatureNotSupportedException(
                        "This connection is read-only. Omit readOnly=true to enable writes.");
            }

            if (createFilterView == null
                    && sql != null
                    && sql.trim().toLowerCase().startsWith("create view")) {
                throw new SQLFeatureNotSupportedException(
                        "CREATE VIEW only supports simple SELECT statements over a single worksheet");
            }

            if (alterTable == null && sql != null && sql.trim().toLowerCase().startsWith("alter table")) {
                throw new SQLFeatureNotSupportedException(
                        "ALTER TABLE supports RENAME TO, ADD COLUMN, DROP COLUMN, and ALTER COLUMN ... RENAME TO");
            }

            if (alterView == null && sql != null && sql.trim().toLowerCase().startsWith("alter view")) {
                throw new SQLFeatureNotSupportedException(
                        "ALTER VIEW supports RENAME TO and AS SELECT over a single worksheet");
            }

            if (dropRelation != null) {
                switch (dropRelation.type()) {
                    case TABLE -> {
                        if (session.snapshot().worksheet(dropRelation.relationName()) == null) {
                            throw new SQLFeatureNotSupportedException(
                                    "DROP TABLE is only supported for worksheet tables");
                        }
                    }
                    case VIEW -> {
                        if (session.filterView(dropRelation.relationName()) == null) {
                            throw new SQLFeatureNotSupportedException(
                                    "DROP VIEW is only supported for Google Sheets views");
                        }
                    }
                }
            }

            if (alterTable != null) {
                if (session.snapshot().worksheet(alterTable.relationName()) == null) {
                    throw new SQLFeatureNotSupportedException("ALTER TABLE is only supported for worksheet tables");
                }
                if (alterTable.type() == AlterTableSpec.Type.DROP_COLUMN
                        && !session.snapshot()
                                .filterViewsForWorksheet(alterTable.relationName())
                                .isEmpty()) {
                    throw new SQLFeatureNotSupportedException(
                            "ALTER TABLE ... DROP COLUMN is not supported for worksheets with dependent views");
                }
            }

            if (alterView != null && session.filterView(alterView.viewName()) == null) {
                throw new SQLFeatureNotSupportedException("ALTER VIEW is only supported for Google Sheets views");
            }

            if (mutation != null) {
                if (session.snapshot().viewNames().contains(mutation.relationName())) {
                    throw new SQLFeatureNotSupportedException("DML against filter views is not supported");
                }
                if (session.snapshot().worksheet(mutation.relationName()) == null) {
                    throw new SQLFeatureNotSupportedException("DML is only supported for worksheet tables");
                }
            }
        }

        private Object dropRelation(String methodName, DropRelationSpec dropRelation) throws SQLException {
            return switch (dropRelation.type()) {
                case TABLE -> dropTable(methodName, dropRelation.relationName());
                case VIEW -> dropView(methodName, dropRelation.relationName());
            };
        }

        private Object alterView(String methodName, AlterViewSpec alterView) throws SQLException {
            FilterViewDefinition existingFilterView = session.filterView(alterView.viewName());
            if (existingFilterView == null) {
                throw new SQLFeatureNotSupportedException("ALTER VIEW is only supported for Google Sheets views");
            }

            return switch (alterView.type()) {
                case RENAME_VIEW -> renameView(methodName, alterView, existingFilterView);
                case REPLACE_VIEW -> replaceView(methodName, alterView, existingFilterView);
            };
        }

        private Object dropTable(String methodName, String relationName) throws SQLException {
            WorksheetSnapshot worksheet = session.snapshot().worksheet(relationName);
            if (worksheet == null) {
                throw new SQLFeatureNotSupportedException("DROP TABLE is only supported for worksheet tables");
            }

            List<FilterViewDefinition> dependentViews = session.snapshot().filterViewsForWorksheet(relationName);
            for (FilterViewDefinition dependentView : dependentViews) {
                dropLocalRelation(dependentView.name(), true);
                session.synchronizer().deleteFilterView(session.url(), dependentView);
                session.unregisterFilterView(dependentView.name());
            }

            dropLocalRelation(relationName, false);
            session.synchronizer().deleteWorksheet(session.url(), worksheet);
            session.unregisterWorksheet(relationName);
            return methodResult(methodName);
        }

        private Object dropView(String methodName, String relationName) throws SQLException {
            FilterViewDefinition filterView = session.filterView(relationName);
            if (filterView == null) {
                throw new SQLFeatureNotSupportedException("DROP VIEW is only supported for Google Sheets views");
            }

            dropLocalRelation(relationName, true);
            session.synchronizer().deleteFilterView(session.url(), filterView);
            session.unregisterFilterView(relationName);
            return methodResult(methodName);
        }

        private Object renameView(String methodName, AlterViewSpec alterView, FilterViewDefinition existingFilterView)
                throws SQLException {
            renameLocalRelation(alterView.viewName(), alterView.targetName());
            FilterViewDefinition updatedFilterView = session.synchronizer()
                    .updateFilterView(
                            session.url(),
                            existingFilterView,
                            new FilterViewDefinition(
                                    existingFilterView.filterViewId(),
                                    alterView.targetName(),
                                    existingFilterView.sourceSheetId(),
                                    existingFilterView.startRowIndex(),
                                    existingFilterView.endRowIndex(),
                                    existingFilterView.startColumnIndex(),
                                    existingFilterView.endColumnIndex(),
                                    existingFilterView.criteria(),
                                    existingFilterView.sortKeys()));
            session.replaceFilterView(alterView.viewName(), updatedFilterView);
            return methodResult(methodName);
        }

        private Object replaceView(String methodName, AlterViewSpec alterView, FilterViewDefinition existingFilterView)
                throws SQLException {
            dropLocalRelation(alterView.viewName(), true);
            try (Statement statement = session.connection().createStatement()) {
                statement.execute(alterView.createSql());
            }
            FilterViewDefinition updatedFilterView = session.synchronizer()
                    .updateFilterView(
                            session.url(),
                            existingFilterView,
                            alterView.replacementSpec().toFilterViewDefinition(existingFilterView));
            session.replaceFilterView(alterView.viewName(), updatedFilterView);
            return methodResult(methodName);
        }

        private void dropLocalRelation(String relationName, boolean preferView) throws SQLException {
            SQLException lastException;
            String firstStatement =
                    (preferView ? "drop view if exists " : "drop table if exists ") + quote(relationName);
            String secondStatement =
                    (preferView ? "drop table if exists " : "drop view if exists ") + quote(relationName);

            try (Statement statement = session.connection().createStatement()) {
                statement.execute(firstStatement);
                return;
            } catch (SQLException exception) {
                lastException = exception;
            }

            try (Statement statement = session.connection().createStatement()) {
                statement.execute(secondStatement);
            } catch (SQLException exception) {
                exception.addSuppressed(lastException);
                throw exception;
            }
        }

        private void renameLocalRelation(String relationName, String newName) throws SQLException {
            String relationType = relationType(relationName);
            String alterStatement = "VIEW".equalsIgnoreCase(relationType)
                    ? "alter view " + quote(relationName) + " rename to " + quote(newName)
                    : "alter table " + quote(relationName) + " rename to " + quote(newName);
            try (Statement statement = session.connection().createStatement()) {
                statement.execute(alterStatement);
            }
        }

        private String relationType(String relationName) throws SQLException {
            try (ResultSet tables = session.connection().getMetaData().getTables(null, null, relationName, null)) {
                while (tables.next()) {
                    String schemaName = tables.getString("TABLE_SCHEM");
                    if (!"INFORMATION_SCHEMA".equalsIgnoreCase(schemaName)) {
                        return tables.getString("TABLE_TYPE");
                    }
                }
            }
            return null;
        }

        private Object methodResult(String methodName) {
            return switch (methodName) {
                case "executeLargeUpdate" -> 0L;
                case "executeUpdate" -> 0;
                default -> false;
            };
        }

        private void synchronize(
                String methodName,
                SqlMutation mutation,
                CreateTableSpec createTable,
                CreateFilterViewSpec createFilterView,
                AlterTableSpec alterTable)
                throws SQLException {
            if (!methodName.startsWith("execute")) {
                return;
            }

            if (createTable != null) {
                WorksheetSnapshot worksheet =
                        OrriDatabase.readWorksheetSnapshot(session.connection(), createTable.relationName(), null);
                WorksheetSnapshot createdWorksheet =
                        session.synchronizer().createWorksheet(session.url(), worksheet, session.connection());
                session.registerWorksheet(createdWorksheet);
                return;
            }

            if (createFilterView != null) {
                FilterViewDefinition createdFilterView = session.synchronizer()
                        .createFilterView(session.url(), createFilterView.toFilterViewDefinition());
                session.registerFilterView(createdFilterView);
                return;
            }

            if (alterTable != null) {
                synchronizeAlterTable(alterTable);
                return;
            }

            if (mutation == null) {
                return;
            }

            int updateCount =
                    switch (delegate) {
                        case Statement statement -> statement.getUpdateCount();
                        default -> -1;
                    };
            if (updateCount > 0) {
                session.syncWorksheet(mutation.relationName());
            }
        }

        private void synchronizeAlterTable(AlterTableSpec alterTable) throws SQLException {
            WorksheetSnapshot currentWorksheet = session.snapshot().worksheet(alterTable.relationName());
            if (currentWorksheet == null) {
                throw new SQLFeatureNotSupportedException("ALTER TABLE is only supported for worksheet tables");
            }

            if (alterTable.type() == AlterTableSpec.Type.RENAME_TABLE) {
                WorksheetSnapshot renamedWorksheet = session.synchronizer()
                        .renameWorksheet(session.url(), currentWorksheet, alterTable.targetName());
                WorksheetSnapshot localWorksheet = OrriDatabase.readWorksheetSnapshot(
                        session.connection(), alterTable.targetName(), renamedWorksheet.sheetId());
                session.replaceWorksheet(alterTable.relationName(), localWorksheet);
                return;
            }

            WorksheetSnapshot updatedWorksheet = OrriDatabase.readWorksheetSnapshot(
                    session.connection(), alterTable.relationName(), currentWorksheet.sheetId());
            session.synchronizer().syncWorksheet(session.url(), updatedWorksheet, session.connection());
            session.replaceWorksheet(alterTable.relationName(), updatedWorksheet);
            OrriDatabase.refreshFilterViews(session.connection(), session.snapshot(), alterTable.relationName());
        }
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static DatabaseMetaData wrapMetaData(DatabaseMetaData delegate, OrriSession session) {
        return (DatabaseMetaData) Proxy.newProxyInstance(
                DatabaseMetaData.class.getClassLoader(),
                new Class[] {DatabaseMetaData.class},
                new MetadataHandler(delegate, session));
    }

    private record MetadataHandler(DatabaseMetaData delegate, OrriSession session) implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return switch (method.getName()) {
                    case "getURL" -> session.url().originalUrl();
                    case "getTables" ->
                        rewriteTableTypes(
                                (ResultSet) method.invoke(delegate, args),
                                session.snapshot().viewNames());
                    case "getTableTypes" -> addViewType((ResultSet) method.invoke(delegate, args));
                    case "unwrap" -> unwrap(args[0]);
                    case "isWrapperFor" -> isWrapperFor(args[0]);
                    default -> method.invoke(delegate, args);
                };
            } catch (InvocationTargetException exception) {
                throw exception.getCause();
            }
        }

        private Object unwrap(Object type) throws SQLException {
            if (type instanceof Class<?> targetType && targetType.isInstance(delegate)) {
                return delegate;
            }
            return delegate.unwrap((Class<?>) type);
        }

        private Object isWrapperFor(Object type) throws SQLException {
            if (type instanceof Class<?> targetType && targetType.isInstance(delegate)) {
                return true;
            }
            return delegate.isWrapperFor((Class<?>) type);
        }
    }

    private static ResultSet rewriteTableTypes(ResultSet delegate, Set<String> viewNames) throws SQLException {
        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        try (delegate) {
            rowSet.populate(delegate);
        }

        while (rowSet.next()) {
            if (viewNames.contains(rowSet.getString("TABLE_NAME"))) {
                rowSet.updateString("TABLE_TYPE", "VIEW");
                rowSet.updateRow();
            }
        }
        rowSet.beforeFirst();
        return rowSet;
    }

    private static ResultSet addViewType(ResultSet delegate) throws SQLException {
        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        try (delegate) {
            rowSet.populate(delegate);
        }

        Set<String> types = new HashSet<>();
        while (rowSet.next()) {
            types.add(rowSet.getString(1));
        }

        if (!types.contains("VIEW")) {
            rowSet.moveToInsertRow();
            rowSet.updateString(1, "VIEW");
            rowSet.insertRow();
            rowSet.moveToCurrentRow();
        }

        rowSet.beforeFirst();
        return rowSet;
    }
}
