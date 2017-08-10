/**
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * <p>
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.inbound.cdc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.util.UUIDGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.wso2.carbon.base.MultitenantConstants;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;

public class CDCPollingConsumer extends GenericPollingConsumer {

    private static final Log log = LogFactory.getLog(CDCPollingConsumer.class);

    private String driverClass;
    private String dbURL;
    private String dbUsername;
    private String dbPassword;
    private String dbScript;
    private String lastUpdatedColumn;
    private Connection connection = null;
    private Statement statement = null;
    private CDCRegistryHandler cdcRegistryHandler;
    private String EIName;
    //    private long timeStamp;
    private String lastUpdated;

    /**
     * @param properties
     * @param name
     * @param synapseEnvironment
     * @param scanInterval
     * @param injectingSeq
     * @param onErrorSeq
     * @param coordination
     * @param sequential
     */
    public CDCPollingConsumer(Properties properties, String name,
                              SynapseEnvironment synapseEnvironment, long scanInterval,
                              String injectingSeq, String onErrorSeq, boolean coordination,
                              boolean sequential) {
        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination,
                sequential);
        cdcRegistryHandler = new CDCRegistryHandler();
        driverClass = properties.getProperty(CDCConstants.DB_DRIVER);
        dbURL = properties.getProperty(CDCConstants.DB_URL);
        dbUsername = properties.getProperty(CDCConstants.DB_USERNAME);
        dbPassword = properties.getProperty(CDCConstants.DB_PASSWORD);
        lastUpdatedColumn = properties.getProperty(CDCConstants.DB_TIMESTAMP_COLUMN);
        EIName = name;
    }

    public boolean inject(OMElement object) {
        org.apache.synapse.MessageContext msgCtx = createMessageContext();

        if (injectingSeq == null || injectingSeq.equals("")) {
            log.error("Sequence name not specified. Sequence : " + injectingSeq);
            return false;
        }
        SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration()
                .getSequence(injectingSeq);
        try {
            msgCtx.getEnvelope().getBody().addChild(object);
            if (seq != null) {
                seq.setErrorHandler(onErrorSeq);
                if (log.isDebugEnabled()) {
                    log.info("injecting message to sequence : " + injectingSeq);
                }
                synapseEnvironment.injectInbound(msgCtx, seq, sequential);
            } else {
                log.error("Sequence: " + injectingSeq + " not found");
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return true;
    }

    private org.apache.synapse.MessageContext createMessageContext() {
        org.apache.synapse.MessageContext msgCtx = synapseEnvironment.createMessageContext();
        org.apache.axis2.context.MessageContext axis2MsgCtx =
                ((org.apache.synapse.core.axis2.Axis2MessageContext) msgCtx).getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(UUIDGenerator.getUUID());
        msgCtx.setProperty(org.apache.axis2.context.MessageContext.CLIENT_API_NON_BLOCKING, true);
        PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
        axis2MsgCtx.setProperty(MultitenantConstants.TENANT_DOMAIN, carbonContext.getTenantDomain());
        return msgCtx;
    }

    /**
     *
     */
    private void executeDBQuery() {
        dbScript = buildQuery(properties.getProperty(CDCConstants.DB_TABLE), lastUpdatedColumn, lastUpdated);
        try {
            statement = connection.prepareStatement(dbScript);
            ResultSet rs = statement.executeQuery(dbScript);
            ResultSetMetaData metaData = rs.getMetaData();
            Boolean dataExist = false;
            while (rs.next()) {
                OMFactory factory = OMAbstractFactory.getOMFactory();
                OMElement result = factory.createOMElement("Record", null);
                int count = metaData.getColumnCount();
                for (int i = 1; i <= count; i++) {
                    String columnName = metaData.getColumnName(i);
                    int type = metaData.getColumnType(i);
                    String columnValue = "";
                    if (type == Types.VARCHAR || type == Types.CHAR) {
                        columnValue = rs.getString(columnName);
                    } else if (type == Types.INTEGER) {
                        columnValue = rs.getInt(columnName) + "";
                    } else if (type == Types.LONGVARCHAR) {
                        columnValue = rs.getLong(columnName) + "";
                    }
                    OMElement messageElement = factory.createOMElement(columnName, null);
                    messageElement.setText(columnValue);
                    result.addChild(messageElement);
                    dataExist = true;
                }
                if (dataExist) {
                    this.inject(result);
                }
            }
        } catch (SQLException e) {
            log.error("Error while capturing the change data " + dbScript);
        }
    }

    /**
     * @param tableName
     * @return
     */
    private String buildQuery(String tableName, String lastUpdatedColumn, String lastUpdatedDate) {
        return "SELECT * FROM " + tableName + " WHERE " + lastUpdatedColumn + " > '" + lastUpdatedDate + "'";
    }

    @Override
    public void destroy() {
        try {
            if (statement != null)
                statement.close();
        } catch (SQLException se2) {
            log.error("");
        }
        try {
            if (connection != null)
                connection.close();
        } catch (SQLException se) {
            log.error("Error while destroying connection to '" + dbURL + "'");
        }
    }

    /**
     * @return
     */
    private void createConnection() {
        try {
            Class.forName(driverClass);
            connection = DriverManager.getConnection(dbURL, dbUsername, dbPassword);
        } catch (ClassNotFoundException e) {
            log.error("Unable to find the driver class " + driverClass);
        } catch (SQLException e) {
            log.error("Error while creating the connection to " + dbURL);
        }
    }

    @Override
    public Object poll() {
        try {
            if (connection == null || connection.isClosed()) {
                createConnection();
            }
            lastUpdated = cdcRegistryHandler.readFromRegistry(EIName).toString();
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat(CDCConstants.REGISTRY_TIME_FORMAT);
            String modifiedDate = sdf.format(cal.getTime());
            executeDBQuery();
            cdcRegistryHandler.writeToRegistry(EIName, modifiedDate);
        } catch (SQLException e) {
            log.error("Error while checking the connection");
        }
        return null;
    }
}
