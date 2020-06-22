/*
 * This file is part of AceQL HTTP.
 * AceQL HTTP: SQL Over HTTP
 * Copyright (C) 2020,  KawanSoft SAS
 * (http://www.kawansoft.com). All rights reserved.
 *
 * AceQL HTTP is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * AceQL HTTP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301  USA
 *
 * Any modifications to this file must keep this entire header
 * intact.
 */
package org.kawanfw.sql.servlet.sql;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.kawanfw.sql.api.server.DatabaseConfigurator;
import org.kawanfw.sql.api.server.firewall.SqlFirewallManager;
import org.kawanfw.sql.servlet.HttpParameter;
import org.kawanfw.sql.servlet.ServerSqlManager;
import org.kawanfw.sql.servlet.sql.json_return.JsonErrorReturn;
import org.kawanfw.sql.servlet.sql.json_return.JsonSecurityMessage;
import org.kawanfw.sql.servlet.sql.json_return.JsonUtil;
import org.kawanfw.sql.util.FrameworkDebug;

/**
 * @author KawanSoft S.A.S
 * <p>
 * Allows to execute the Statement or Prepared Statement on the Server
 * as executeQuery() or executeUpdate()
 */
public class ServerStatement {
    public static String CR_LF = System.getProperty("line.separator");
    private static boolean DEBUG = true;  //FrameworkDebug.isSet(ServerStatement.class);
    private Connection connection = null;
    // private String username = null;

    /**
     * The http request
     */
    private HttpServletRequest request;

    private HttpServletResponse response;

    private Boolean doPrettyPrinting;

    private List<SqlFirewallManager> sqlFirewallManagers = new ArrayList<>();

    /**
     * Default Constructor
     *
     * @param request               the http request
     * @param response              the http servlet response
     * @param sqlFirewallManagers
     * @param connection
     * @param sqlOrderAndParmsStore the Sql order and parms
     */

    public ServerStatement(HttpServletRequest request, HttpServletResponse response,
                           List<SqlFirewallManager> sqlFirewallManagers, Connection connection) throws SQLException {
        this.request = request;
        this.response = response;
        this.sqlFirewallManagers = sqlFirewallManagers;
        this.connection = connection;
        doPrettyPrinting = true; // Always pretty printing

    }

    /**
     * Execute the SQL query or update. <br>
     *
     * @param out
     * @throws FileNotFoundException
     * @throws IOException
     * @throws SQLException
     */
    public void executeQueryOrUpdate(OutputStream out) throws FileNotFoundException, IOException, SQLException {

        // Get the GZIP Stream if necessary
        OutputStream outFinal = null;

        try {

            outFinal = getFinalOutputStream(out);

            // Execute it
            if (isPreparedStatement()) {
                executePrepStatement(outFinal);
            } else {
                executeStatement(outFinal);
            }
        } catch (SecurityException e) {
            JsonErrorReturn errorReturn = new JsonErrorReturn(response, HttpServletResponse.SC_FORBIDDEN,
                    JsonErrorReturn.ERROR_ACEQL_UNAUTHORIZED, e.getMessage());
            ServerSqlManager.writeLine(outFinal, errorReturn.build());
        } catch (SQLException e) {
            JsonErrorReturn errorReturn = new JsonErrorReturn(response, HttpServletResponse.SC_BAD_REQUEST,
                    JsonErrorReturn.ERROR_JDBC_ERROR, e.getMessage());
            ServerSqlManager.writeLine(outFinal, errorReturn.build());
        } catch (Exception e) {
            JsonErrorReturn errorReturn = new JsonErrorReturn(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    JsonErrorReturn.ERROR_ACEQL_FAILURE, e.getMessage(), ExceptionUtils.getStackTrace(e));
            ServerSqlManager.writeLine(outFinal, errorReturn.build());
        } finally {

            // IOUtils.closeQuietly(outFinal);

            if (outFinal != null) {
                try {
                    outFinal.close();
                } catch (Exception e) {
                    // e.printStackTrace();
                }
            }

        }
    }

    /**
     * Get the OutputStream to use. A regular one or a GZIP_RESULT one
     *
     * @param file
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    private OutputStream getFinalOutputStream(OutputStream out) throws FileNotFoundException, IOException {

        String gzipResult = request.getParameter(HttpParameter.GZIP_RESULT);
        boolean doGzip = Boolean.parseBoolean(gzipResult);

        // No GZIP if execute update
        if (isExecuteUpdate()) {
            doGzip = false;
        }

        if (doGzip) {
            GZIPOutputStream gZipOut = new GZIPOutputStream(out);
            return gZipOut;
        } else {
            OutputStream outFinal = out;
            return outFinal;

        }
    }

    private boolean isPreparedStatement() {
        String preparedStatement = request.getParameter(HttpParameter.PREPARED_STATEMENT);
        return Boolean.parseBoolean(preparedStatement);

    }

    /**
     * Execute the passed SQL Statement and return: <br>
     * - The result set as a List of Maps for SELECT statements. <br>
     * - The return code for other statements
     *
     * @param sqlOrder the qsql order
     * @param sqlParms the sql parameters
     * @param out      the writer where to write to result set output
     * @throws SQLException
     */
    private void executePrepStatement(OutputStream out) throws SQLException, IOException {

        String username = request.getParameter(HttpParameter.USERNAME);
        String database = request.getParameter(HttpParameter.DATABASE);
        String sqlOrder = request.getParameter(HttpParameter.SQL);

        debug("sqlOrder        : " + sqlOrder);

        PreparedStatement preparedStatement = null;

        // Class to set all the statement parameters
        ServerPreparedStatementParameters serverPreparedStatementParameters = null;

        try {

            if (sqlOrder == null || sqlOrder.isEmpty()) {
                throw new SQLException("A 'sql' statement is required.");
            }
            preparedStatement = connection.prepareStatement(sqlOrder);

            debug("before ServerPreparedStatementParameters");

            serverPreparedStatementParameters = new ServerPreparedStatementParameters(preparedStatement, request);

            try {
                serverPreparedStatementParameters.setParameters();
            } catch (IllegalArgumentException e) {
                JsonErrorReturn errorReturn = new JsonErrorReturn(response, HttpServletResponse.SC_BAD_REQUEST,
                        JsonErrorReturn.ERROR_ACEQL_ERROR, e.getMessage());
                ServerSqlManager.writeLine(out, errorReturn.build());
                return;
            }

            // Throws a SQL exception if the order is not authorized:
            debug("before new SqlSecurityChecker()");

            String ipAddress = request.getRemoteAddr();

            boolean isAllowedAfterAnalysis = false;
            for (SqlFirewallManager sqlFirewallManager : sqlFirewallManagers) {
                isAllowedAfterAnalysis = sqlFirewallManager.allowSqlRunAfterAnalysis(username, database, connection, ipAddress,
                        sqlOrder, isPreparedStatement(), serverPreparedStatementParameters.getParameterValues());
                if (!isAllowedAfterAnalysis) {
                    List<Object> parameterValues = new ArrayList<>();
                    sqlFirewallManager.runIfStatementRefused(username, database, connection, ipAddress, false, sqlOrder, parameterValues);
                    break;
                }
            }

            if (!isAllowedAfterAnalysis) {
                String message = JsonSecurityMessage.prepStatementNotAllowedBuild(sqlOrder,
                        "Prepared Statement not allowed", serverPreparedStatementParameters.getParameterTypes(),
                        serverPreparedStatementParameters.getParameterValues(), doPrettyPrinting);
                throw new SecurityException(message);
            }

            debug("before executeQuery() / executeUpdate()");

            if (isExecuteUpdate()) {

                //boolean allowExecuteUpdate = DatabaseConfiguratorCall.allowExecuteUpdate(databaseConfigurator, username, connection);

                for (SqlFirewallManager sqlFirewallManager : sqlFirewallManagers) {
                    isAllowedAfterAnalysis = sqlFirewallManager.allowExecuteUpdate(username, database, connection);
                    if (!isAllowedAfterAnalysis) {
                        List<Object> parameterValues = new ArrayList<>();
                        sqlFirewallManager.runIfStatementRefused(username, database, connection, ipAddress, false,
                                sqlOrder, parameterValues);

                        String message = JsonSecurityMessage.prepStatementNotAllowedBuild(sqlOrder,
                                "Prepared Statement not allowed for executeUpdate",
                                serverPreparedStatementParameters.getParameterTypes(),
                                serverPreparedStatementParameters.getParameterValues(), doPrettyPrinting);

                        throw new SecurityException(message);
                    }
                }

                int rc = preparedStatement.executeUpdate();

                StringWriter sw = new StringWriter();
                JsonGeneratorFactory jf = JsonUtil.getJsonGeneratorFactory(JsonUtil.DEFAULT_PRETTY_PRINTING);
                JsonGenerator gen = jf.createGenerator(sw);

                gen.writeStartObject().write("status", "OK").write("row_count", rc).writeEnd();
                gen.close();

                ServerSqlManager.write(out, sw.toString());

            } else {

                ResultSet rs = null;

                try {

                    rs = preparedStatement.executeQuery();

                    JsonGeneratorFactory jf = JsonUtil.getJsonGeneratorFactory(doPrettyPrinting);

                    JsonGenerator gen = jf.createGenerator(out);
                    gen.writeStartObject().write("status", "OK");

                    ResultSetWriter resultSetWriter = new ResultSetWriter(request, username, sqlOrder, gen);
                    resultSetWriter.write(rs);

                    ServerSqlManager.writeLine(out);

                    gen.writeEnd(); // .write("status", "OK")
                    gen.flush();
                    gen.close();

		    /*
		    String file = FrameworkFileUtil.getKawansoftTempDir() + File.separator + "result_set_out_new.txt";
		    try (OutputStream outFile = new BufferedOutputStream(new FileOutputStream(file))) {
			JsonGenerator gen = jf.createGenerator(outFile);
			gen.writeStartObject().write("status", "OK");

			ResultSetWriter resultSetWriter = new ResultSetWriter(request, outFile, username, sqlOrder,
				gen);
			resultSetWriter.write(rs);

			ServerSqlManager.writeLine(outFile);

			gen.writeEnd(); // .write("status", "OK")
			gen.flush();
			gen.close();
		    }

		    try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
			IOUtils.copy(in, out);
		    }
		    */


                } finally {

                    if (rs != null) {
                        rs.close();
                    }
                }
            }
        } catch (SQLException e) {

            String message = StatementFailure.prepStatementFailureBuild(sqlOrder, e.toString(),
                    serverPreparedStatementParameters.getParameterTypes(),
                    serverPreparedStatementParameters.getParameterValues(), doPrettyPrinting);

            LoggerUtil.log(request, e, message);
            throw e;
        } finally {
            // Close the ServerPreparedStatementParameters
            if (serverPreparedStatementParameters != null) {
                serverPreparedStatementParameters.close();
            }

            if (preparedStatement != null) {
                preparedStatement.close();
            }

            // Clean all
            serverPreparedStatementParameters = null;

        }
    }

    /**
     * Execute the passed SQL Statement and return: <br>
     * - The result set as a List of Maps for SELECT statements. <br>
     * - The return code for other statements
     *
     * @param out the writer where to write to result set output
     * @throws SQLException
     */
    private void executeStatement(OutputStream out) throws SQLException, IOException {

        String username = request.getParameter(HttpParameter.USERNAME);
        String database = request.getParameter(HttpParameter.DATABASE);
        String sqlOrder = request.getParameter(HttpParameter.SQL);
        debug("sqlOrder   : " + sqlOrder);

        Statement statement = null;

        DatabaseConfigurator databaseConfigurator = ServerSqlManager.getDatabaseConfigurator(database);

        try {

            if (sqlOrder == null || sqlOrder.isEmpty()) {
                throw new SQLException("A 'sql' statement is required.");
            }

            statement = connection.prepareStatement(sqlOrder);
            // Throws a SQL exception if the order is not authorized:
            debug("before new SqlSecurityChecker()");

            String ipAddress = request.getRemoteAddr();
            boolean isAllowed = false;

            SqlFirewallManager sqlFirewallOnDeny = null;
            for (SqlFirewallManager sqlFirewallManager : sqlFirewallManagers) {
                sqlFirewallOnDeny = sqlFirewallManager;
                isAllowed = sqlFirewallManager.allowStatementClass(username, database, connection);
                if (!isAllowed) {
                    break;
                }

                isAllowed = sqlFirewallManager.allowSqlRunAfterAnalysis(username, database,
                        connection, ipAddress, sqlOrder, isPreparedStatement(), new Vector<Object>());
                if (!isAllowed) {
                    break;
                }
            }

            if (!isAllowed) {
                List<Object> parameterValues = new ArrayList<>();
                sqlFirewallOnDeny.runIfStatementRefused(username, database, connection, ipAddress,
                        false, sqlOrder, parameterValues);

                String message = JsonSecurityMessage.statementNotAllowedBuild(sqlOrder, "Statement not allowed",
                        doPrettyPrinting);
                throw new SecurityException(message);
            }

            statement = connection.createStatement();

            debug("before executeQuery() / executeUpdate(sqlOrder)");

            if (isExecuteUpdate()) {

                for (SqlFirewallManager sqlFirewallManager : sqlFirewallManagers) {
                    isAllowed = sqlFirewallManager.allowExecuteUpdate(username, database, connection);
                    if (!isAllowed) {
                        List<Object> parameterValues = new ArrayList<>();
                        sqlFirewallManager.runIfStatementRefused(username, database, connection, ipAddress, false,
                                sqlOrder, parameterValues);

                        String message = JsonSecurityMessage.statementNotAllowedBuild(sqlOrder,
                                "Statement not allowed for for executeUpdate", doPrettyPrinting);
                        throw new SecurityException(message);

                    }
                }

                int rc = -1;

                rc = statement.executeUpdate(sqlOrder);

                StringWriter sw = new StringWriter();

                JsonGeneratorFactory jf = JsonUtil.getJsonGeneratorFactory(JsonUtil.DEFAULT_PRETTY_PRINTING);
                JsonGenerator gen = jf.createGenerator(sw);

                gen.writeStartObject().write("status", "OK").write("row_count", rc).writeEnd();
                gen.close();

                ServerSqlManager.write(out, sw.toString());

            } else {
                ResultSet rs = null;

                try {

                    ServerSqlUtil.setMaxRowsToReturn(username, database, statement, databaseConfigurator);
                    debug("sqlorder: " + sqlOrder);

                    rs = statement.executeQuery(sqlOrder);

                    JsonGeneratorFactory jf = JsonUtil.getJsonGeneratorFactory(doPrettyPrinting);

                    JsonGenerator gen = jf.createGenerator(out);
                    gen.writeStartObject().write("status", "OK");

                    ResultSetWriter resultSetWriter = new ResultSetWriter(request, username, sqlOrder, gen);
                    resultSetWriter.write(rs);

                    ServerSqlManager.writeLine(out);

                    gen.writeEnd(); // .write("status", "OK")
                    gen.flush();
                    gen.close();

                } finally {
                    if (rs != null) {
                        rs.close();
                    }
                }
            }
        } catch (SQLException e) {

            String message = StatementFailure.statementFailureBuild(sqlOrder, e.toString(), doPrettyPrinting);

            LoggerUtil.log(request, e, message);
            throw e;

        } finally {
            // NO! IOUtils.closeQuietly(out);

            if (statement != null) {
                statement.close();
            }
        }
    }

    private boolean isExecuteUpdate() {

        return request.getParameter(HttpParameter.ACTION).equals(HttpParameter.EXECUTE_UPDATE);
    }

    /**
     * @param s
     */

    protected void debug(String s) {
        if (DEBUG) {
            System.out.println(new Date() + " " + s);
        }
    }

}
