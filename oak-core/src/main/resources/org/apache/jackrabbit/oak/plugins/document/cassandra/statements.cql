# Statements for Cassandra Document Store.
# Anything after # is a comment.
# lines ending with a \ are continuation lines.
# __DBNAME__ gets replaced by the dbname.
# Each line starts with a key, terminated by :
#  Keys:
#     cddl: a non distructive create statement, executed in the order they appear.
#     dddl: a safe drop statement, executed in the order they appear.
#     pcql: a prepared statement, second field is the key terminated by a :
#     ssql: a non prepared statement,  second field is the key terminated by a :

# create statements.
cddl: CREATE KEYSPACE IF NOT EXISTS __DBNAME__ WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}

#cddl: DROP MATERIALIZED VIEW IF EXISTS __DBNAME__.nodes_by_parentid;
#cddl: DROP MATERIALIZED VIEW IF EXISTS __DBNAME__.nodes_by_modified;
#cddl: DROP TABLE IF EXISTS __DBNAME__.nodes;
cddl: CREATE TABLE IF NOT EXISTS __DBNAME__.nodes (
          id text PRIMARY KEY,
          modified bigint,
          lastrev text,
          modcount bigint,
          cmodcount bigint,
          children boolean,
          deletedonce boolean,
          parentid text,
          revisions list<text>,  # value is the revision+key json encoded.
          data map<text, text>);  # map of key and value where value is json for the revision, to update replace the whole value.

# cddl: DROP INDEX IF EXISTS __DBNAME__.nodes_modifed;
# cddl: DROP INDEX IF EXISTS __DBNAME__.nodes_modifed_idx;
# cddl: CREATE INDEX on __DBNAME__.nodes(modified);

cddl: CREATE MATERIALIZED VIEW IF NOT EXISTS __DBNAME__.nodes_by_modified
         as select * from __DBNAME__.nodes
         where modified is not null
         primary key(modified, id);

cddl: CREATE MATERIALIZED VIEW IF NOT EXISTS __DBNAME__.nodes_by_parentid
         as select * from __DBNAME__.nodes
         where parentid is not null
         primary key(parentid, id);

cddl: CREATE TABLE IF NOT EXISTS __DBNAME__.clusterNodes (
    id text PRIMARY KEY);

cddl: CREATE TABLE IF NOT EXISTS __DBNAME__.settings (
    id text PRIMARY KEY);

cddl: CREATE TABLE IF NOT EXISTS __DBNAME__.journal (
    id text PRIMARY KEY);


cddl: CREATE TABLE IF NOT EXISTS __DBNAME__.blob (
    id text PRIMARY KEY);


# cddl: CREATE OR REPLACE FUNCTION __DBNAME__.fmax (a double, b double) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS 'return Math.max(a, b);';
# cddl: CREATE OR REPLACE FUNCTION __DBNAME__.lmax (a bigint, b bigint) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return Math.max(a, b);';

# Mapped fiedls, mfld:<collection>.<collectionfield>:<cql field>
mfld:nodes._id:id
mfld:nodes._parentid:parentid
mfld:nodes._modified:modified
mfld:nodes._lastrev:lastrev
mfld:nodes._modCount:modcount
mfld:nodes._collisionsModCount:cmodcount
mfld:nodes._deletedOnce:deletedonce
mfld:nodes.children:children

ifld:nodes._id:id
ifld:nodes._parentid:parentid
ifld:nodes._modified:modified




# drop statements, used in uinit tests.

# dddl: drop materialized view if exists  __DBNAME__.nodes_by_modified;
# dddl: drop table if exists  __DBNAME__.nodes;
# dddl: drop table if exists  __DBNAME__.clusterNodes;
# dddl: drop table if exists  __DBNAME__.settings;
# dddl: drop table if exists  __DBNAME__.journal;
# dddl: drop table if exists  __DBNAME__.blob;
# dddl: drop function if exists __DBNAME__.lmax;
# dddl: drop function if exists __DBNAME__.fmax;

dddl: TRUNCATE  __DBNAME__.nodes;
dddl: TRUNCATE  __DBNAME__.clusterNodes;
dddl: TRUNCATE  __DBNAME__.settings;
dddl: TRUNCATE  __DBNAME__.journal;
dddl: TRUNCATE  __DBNAME__.blob;



# prepared statements.
pcql:deleteone_nodes: delete from __DBNAME__.nodes where id = ?
pcql:deletemany_nodes: delete from __DBNAME__.nodes where id IN ?
scql:deletemanyconditional_nodes: delete from __DBNAME__.nodes where __0__;
scql:insert_nodes: insert into __DBNAME__.nodes ( __0__ ) values ( __1__ );
scql:update_nodes: update __DBNAME__.nodes set  __0__  where id = ?;
pcql:findbyids_nodes: select * from __DBNAME__.nodes where id in ?;
scql:findbyparentid_nodes: select * from __DBNAME__.nodes_by_parentid where parentid = ? and __0__ >= ? limit __1__;
scql:findbyparentidnoproperty_nodes: select * from __DBNAME__.nodes_by_parentid where parentid = ? limit __0__;
scql:findbyallnoproperty_nodes: select * from __DBNAME__.nodes;
scql:findbyall_nodes: select * from __DBNAME__.nodes where __0__ >= ?;


pcql:deleteone_clusterNodes: delete from __DBNAME__.clusterNodes where id = ?
pcql:deletemany_clusterNodes: delete from __DBNAME__.clusterNodes where id IN ?
scql:deletemanyconditional_clusterNodes: delete from __DBNAME__.clusterNodes
scql:insert_clusterNodes: insert into __DBNAME__.clusterNodes ( __0__ ) values ( __1__ );
scql:update_clusterNodes: update __DBNAME__.clusterNodes set  __0__  where id = ?;
pcql:findbyids_clusterNodes: select * from __DBNAME__.clusterNodes where id in ?;

pcql:deleteone_journal: delete from __DBNAME__.journal where id = ?
pcql:deletemany_journal: delete from __DBNAME__.journal where id IN ?
scql:deletemanyconditional_journal: delete from __DBNAME__.journal
scql:insert_journal: insert into __DBNAME__.journal ( __0__ ) values ( __1__ );
scql:update_journal: update __DBNAME__.journal set  __0__  where id = ?;
pcql:findbyids_journal: select * from __DBNAME__.journal where id in ?;

pcql:deleteone_settings: delete from __DBNAME__.settings where id = ?
pcql:deletemany_settings: delete from __DBNAME__.settings where id IN ?
scql:deletemanyconditional_settings: delete from __DBNAME__.settings
scql:insert_settings: insert into __DBNAME__.settings ( __0__ ) values ( __1__ );
scql:update_settings: update __DBNAME__.settings set  __0__  where id = ?;
pcql:findbyids_settings: select * from __DBNAME__.settings where id in ?;


