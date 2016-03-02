package org.apache.jackrabbit.oak.plugins.document.cassandra;

import com.datastax.driver.core.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Created by ieb on 15/02/2016.
 */
public class C8Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(C8Client.class);
    private static Map<String, C8Client> instances = new ConcurrentHashMap<String, C8Client>();
    private final Session session;
    private final String nodes;
    private Cluster cluster;
    private final String dbName;
    private final String[] contactPoints;
    private List<String> createStatements;
    private Map<String, String> preparedStatementsSource;
    private Map<String, PreparedStatement> preparedStatements;
    private List<String> dropStatements;
    private Map<String, String> statements;
    private Map<String, String> mappedFields;
    // C8 sessions dont like re-preparing statements so try and cache them.
    private Map<String, PreparedStatement> generatedPreparedStatements = new ConcurrentHashMap<String, PreparedStatement>();
    private  Map<String, String> indexedFields;


    public static C8Client getInstance(@Nonnull String dbName, @Nonnull String nodes) {
        String key = dbName+":"+nodes;
        C8Client client = instances.get(key);
        if (client == null) {
            client = new C8Client(dbName, nodes);
            instances.put(key, client);
        }
        return client;
    }

    private static void drop(C8Client client) {
        instances.remove(client.dbName + ":" + client.nodes);
    }

    private C8Client(@Nonnull String dbName, @Nonnull String nodes) {
        this.dbName = dbName;
        this.nodes = nodes;
        this.contactPoints = nodes.split(",");
        cluster = Cluster.builder().addContactPoints(contactPoints).build();
        Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: {}",
                metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            LOGGER.info("Datatacenter: {}; Host: {}; Rack: {}",
                    new Object[]{host.getDatacenter(), host.getAddress(), host.getRack()});
        }
        session = cluster.connect();
        loadStatements();
        createDatabase();
    }


    private void loadStatements() {
        try {
            String dbName = getDbName();
            BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("statements.cql")));
            ImmutableMap.Builder<String, String> pbuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, String> sbuilder = ImmutableMap.builder();
            ImmutableList.Builder<String> cbuilder = ImmutableList.builder();
            ImmutableList.Builder<String> dbuilder = ImmutableList.builder();
            ImmutableMap.Builder<String, String> fbuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, String> ibuilder = ImmutableMap.builder();
            StringBuilder statement = new StringBuilder();
            for (;;) {
                String line = br.readLine();
                if (line == null) {
                    processLine(statement.toString(),pbuilder, sbuilder, cbuilder, dbuilder, fbuilder, ibuilder);
                    break;
                }
                String[] withoutComments = line.split("#");
                if (!withoutComments[0].startsWith(" ")) {
                    String statementLine = statement.toString().trim();
                    if ( statementLine.length() > 0 ) {
                        processLine(statementLine,pbuilder, sbuilder, cbuilder, dbuilder, fbuilder, ibuilder);
                    }
                    statement = new StringBuilder();
                }
                statement.append(withoutComments[0]);
            }
            br.close();
            preparedStatementsSource = pbuilder.build();
            statements = sbuilder.build();
            createStatements = cbuilder.build();
            dropStatements = dbuilder.build();
            mappedFields = fbuilder.build();
            indexedFields = ibuilder.build();
        } catch (IOException e) {
            throw new IllegalArgumentException("Input statements.cql is invalid ",e);
        }


    }

    private void processLine(String statement, ImmutableMap.Builder<String, String> pbuilder,
                             ImmutableMap.Builder<String, String> sbuilder, ImmutableList.Builder<String> cbuilder,
                             ImmutableList.Builder<String> dbuilder, ImmutableMap.Builder<String, String> fbuilder,
                             ImmutableMap.Builder<String, String> ibuilder) {
        LOGGER.debug("Processing statement {} ", statement);
        if (statement.startsWith("pcql:")) {
            String[] parts = statement.split(":", 3);
            String st = parts[2].replaceAll("__DBNAME__", dbName);
            LOGGER.debug("Processing statement {} ", st);
            pbuilder.put(parts[1], st);
        } else if (statement.startsWith("scql:")) {
            String[] parts = statement.split(":", 3);
            String st = parts[2].replaceAll("__DBNAME__", dbName);
            LOGGER.debug("Processing statement {} ", st);
            sbuilder.put(parts[1], st);
        } else if (statement.startsWith("cddl:")) {
            String[] parts = statement.split(":", 2);
            String st = parts[1].replaceAll("__DBNAME__", dbName);
            LOGGER.debug("Processing statement {} ", st);
            cbuilder.add(st);
        } else if (statement.startsWith("dddl:")) {
            String[] parts = statement.split(":", 2);
            String st = parts[1].replaceAll("__DBNAME__", dbName);
            LOGGER.debug("Processing statement {} ", st);
            dbuilder.add(st);
        } else if (statement.startsWith("mfld:")) {
            String[] parts = statement.split(":", 3);
            LOGGER.debug("Processing mapping {} {}", parts[1], parts[2]);
            fbuilder.put(parts[1], parts[2]);
        } else if (statement.startsWith("ifld:")) {
            String[] parts = statement.split(":", 3);
            LOGGER.debug("Processing index mapping {} {}", parts[1], parts[2]);
            ibuilder.put(parts[1], parts[2]);
        }
    }


    private void checkCluster() {
        if ( cluster == null || session == null) {
            throw new IllegalStateException("Not connected to a cluster");
        }
    }

    public Session getSession() {
        checkCluster();
        return session;
    }

    public void close() {
        if (cluster != null) {
            checkCluster();
            drop(this);
            session.close();
            cluster.close();
            cluster = null;
            LOGGER.info("Client Closed");
        }

    }

    public void createDatabase() {
        checkCluster();
        LOGGER.info("Creating Cassandra Keyspace {}  ", dbName);
        for (String createStatement : createStatements ) {
            LOGGER.info("Executing {}   ", createStatement);
            session.execute(createStatement);
        }
        ImmutableMap.Builder<String, PreparedStatement> pbuilder = ImmutableMap.builder();
        for (Map.Entry<String, String> e : preparedStatementsSource.entrySet()) {
            pbuilder.put(e.getKey(), session.prepare(e.getValue()));
        }
        preparedStatements = pbuilder.build();
    }

    public void dropCollections() {
        checkCluster();
        LOGGER.debug("Dropping Cassandra Collections {}  ", dbName);
        for (String dropStatement : dropStatements) {
            session.execute(dropStatement);
        }
        LOGGER.info("Done Dropping Cassandra Collections {}  ", dbName);
    }

    public void ping() {
        checkCluster();
        LOGGER.debug("Ping Cassandra at {}  ", Arrays.toString(contactPoints));
        ResultSet results = session.execute("select * from system.peers");
        if ( results != null) {
            LOGGER.info("Done Ping Cassandra at {}  ", Arrays.toString(contactPoints));
            return;
        }
        throw new IllegalStateException("Unable to ping Cassandra Cluster at "+ Arrays.toString(contactPoints));
    }


    public void dropDatabase() {
        checkCluster();
        LOGGER.info("Dropping Cassandra Keyspace {}  ", dbName);
        // session.execute("drop keyspace if exists  " + dbName );
        LOGGER.info("Done Dropping Cassandra Keyspace {}  ", dbName);

    }

    public String getDbName() {
        return dbName;
    }

    public <T extends Document> BoundStatement getBoundStatement(Collection<T> collection, String key) {
        PreparedStatement ps = preparedStatements.get(key+"_"+collection.toString());
        if (ps == null) {
            throw new IllegalArgumentException("No Prepared statement found for key "+key+"_"+collection.toString());
        }
        LOGGER.info("Binding {} ",key+"_"+collection.toString());
        return new BoundStatement(ps);
    }

    public <T extends Document> String getStatement(Collection<T> collection, String key, String ... additional) {
        String st = statements.get(key + "_" + collection.toString());
        if (st == null) {
            throw new IllegalArgumentException("No Statement Template found for key " + key + "_" + collection.toString());
        }
        return processStatement(st, additional);
    }

    public <T extends Document> PreparedStatement getPreparedStatement(Collection<T> collection, String key, String ... additional) {
        String statement = getStatement(collection, key, additional);
        PreparedStatement pst = generatedPreparedStatements.get(statement);
        if ( pst == null) {
            LOGGER.info("created Prepared statement {} ", statement);
            pst = session.prepare(statement);
            generatedPreparedStatements.put(statement,pst);
        }
        return pst;
    }

    private String processStatement(String statementTemplate, String[] params) {
        StringBuilder sb = new StringBuilder(statementTemplate);
        int i = sb.indexOf("__");
        while(i >= 0) {
            int j = sb.indexOf("__",i+2);
            int p = Integer.parseInt(sb.substring(i + 2, j).toString());
            sb.replace(i,j+2,params[p]);
            i = sb.indexOf("__",i+params[p].length());
        }
        return sb.toString();
    }

    public <T extends Document> String mapField(Collection<T> collection, String name) {
        String mf =  mappedFields.get(collection.toString()+"."+name);
        if (mf == null) {
            LOGGER.warn("No Fields mapping for {} ",(collection.toString()+"."+name));
        }
        return mf;
    }

    public <T extends Document> String mapIndexedFiled(Collection<T> collection, String name) {
        String mf =  indexedFields.get(collection.toString()+"."+name);
        if (mf == null) {
            LOGGER.warn("No Indexed Fields mapping for {} ",(collection.toString()+"."+name));
        }
        return mf;
    }

    public String join(List<String> terms, String seperator) {
        if ( terms.size() == 0) {
            return " ";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(terms.get(0));
        for (int i = 1; i < terms.size(); i++ ) {
            sb.append(seperator).append(terms.get(i));
        }
        return sb.toString();
    }


}
