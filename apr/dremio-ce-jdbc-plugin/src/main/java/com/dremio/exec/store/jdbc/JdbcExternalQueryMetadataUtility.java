package com.dremio.exec.store.jdbc;

import javax.sql.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.record.*;
import com.dremio.common.concurrent.*;
import com.dremio.exec.tablefunctions.*;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.runtime.*;
import java.util.concurrent.*;
import com.google.common.primitives.*;
import com.dremio.common.exceptions.*;
import java.util.*;
import com.dremio.exec.store.jdbc.dialect.*;
import java.util.function.*;
import java.util.stream.*;
import java.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.*;
import com.google.common.annotations.*;
import org.slf4j.*;

public final class JdbcExternalQueryMetadataUtility
{
    private static final Logger LOGGER;
    private static final long METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS;
    
    public static BatchSchema getBatchSchema(final DataSource source, final JdbcDremioSqlDialect dialect, final String query, final String sourceName) {
        final ExecutorService executor = Executors.newSingleThreadExecutor((ThreadFactory)new NamedThreadFactory(Thread.currentThread().getName() + ":jdbc-eq-metadata"));
        final Callable<BatchSchema> retrieveMetadata = () -> getExternalQueryMetadataFromSource(source, dialect, query, sourceName);
        final Future<BatchSchema> future = executor.submit(retrieveMetadata);
        return handleMetadataFuture(future, executor, JdbcExternalQueryMetadataUtility.METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS);
    }
    
    static BatchSchema handleMetadataFuture(final Future<BatchSchema> future, final ExecutorService executor, final long timeout) {
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            JdbcExternalQueryMetadataUtility.LOGGER.debug("Timeout while fetching metadata", (Throwable)e);
            throw newValidationError((Resources.ExInst<SqlValidatorException>)DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError((Throwable)e));
        }
        catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
            throw newValidationError((Resources.ExInst<SqlValidatorException>)DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError((Throwable)e2));
        }
        catch (ExecutionException e3) {
            final Throwable cause = e3.getCause();
            if (cause instanceof CalciteContextException) {
                throw (CalciteContextException)cause;
            }
            throw newValidationError((Resources.ExInst<SqlValidatorException>)DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(cause));
        }
        finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }
    
    private static BatchSchema getExternalQueryMetadataFromSource(final DataSource source, final JdbcDremioSqlDialect dialect, final String query, final String sourceName) throws SQLException {
        final Connection conn = source.getConnection();
        Throwable x0 = null;
        try {
            final PreparedStatement stmt = conn.prepareStatement(query);
            Throwable x2 = null;
            try {
                stmt.setQueryTimeout(Ints.saturatedCast(TimeUnit.MILLISECONDS.toSeconds(JdbcExternalQueryMetadataUtility.METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS)));
                final ResultSetMetaData metaData = stmt.getMetaData();
                if (metaData == null) {
                    throw newValidationError((Resources.ExInst<SqlValidatorException>)DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryInvalidError(sourceName));
                }
                final List<JdbcToFieldMapping> mappings = dialect.getDataTypeMapper().mapJdbcToArrowFields(null, null, message -> {
                    throw UserException.invalidMetadataError().addContext(message).buildSilently();
                }, conn, metaData, null, true);
                return BatchSchema.newBuilder().addFields((Iterable)mappings.stream().map(JdbcToFieldMapping::getField).collect(Collectors.toList())).build();
            }
            catch (Throwable t) {
                x2 = t;
                throw t;
            }
            finally {
                if (stmt != null) {
                    $closeResource(x2, stmt);
                }
            }
        }
        catch (Throwable t2) {
            x0 = t2;
            throw t2;
        }
        finally {
            if (conn != null) {
                $closeResource(x0, conn);
            }
        }
    }
    
    @VisibleForTesting
    static CalciteContextException newValidationError(final Resources.ExInst<SqlValidatorException> exceptionExInst) {
        return SqlUtil.newContextException(SqlParserPos.ZERO, (Resources.ExInst)exceptionExInst);
    }
    
    private static /* synthetic */ void $closeResource(final Throwable x0, final AutoCloseable x1) {
        if (x0 != null) {
            try {
                x1.close();
            }
            catch (Throwable t) {
                x0.addSuppressed(t);
            }
        }
        else {
            try {
				x1.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)JdbcExternalQueryMetadataUtility.class);
        METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS = TimeUnit.SECONDS.toMillis(120L);
    }
}
