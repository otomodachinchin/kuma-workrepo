#!/usr/bin/python

from __future__ import print_function
import re
import traceback
import uuid
from ctrl.logger import Logger
import os

# from pprint import pprint
from ctrl import dbObj


class SqlNonString(object):

    def __init__(self, arg):
        self.value = arg

    def __str__(self):
        return str(self.value)


class ApiDbClusterInfo(object):

    def nodeIdConcatForQuery(self):
        field = 'na_cluster_node_id=\''
        return field + ('\' or ' + field).join([str(x.id) for x in self.argCluster.nodes]) + '\''

    def check_duplication_in_na_cluster(self):
        # check duplication in na_cluster
        try:
            result_get_na_cluster = dbObj._get_na_cluster(
                self.na_cluster_name, '*')
        except:
            result_get_na_cluster = ''

        if result_get_na_cluster:
            return True
        return False

    def volume_type_validation(self):
        # volume_type validation between volume_type and na_cluster
        resChkNfsClusters = dbObj.check_specified_cluster_type_clusters(
            self.cluster_type)
        resGetNfsClusters = dbObj.get_volume_type(
            self.volume_type_id)
        if bool(resChkNfsClusters) != bool(resGetNfsClusters):
            Logger().warn(
                'volume_type validation between volume_type '
                'and na_cluster failed. proceeding...')
            return 1
        elif not resGetNfsClusters:
            self.isFirstVolumeTypeCluster = True
        return 0

    def az_validation(self):
        # az validation between availability_zone and na_cluster
        clusters_in_az = dbObj.get_cluster_ids(
            az_id=self.availability_zone_id)
        az = dbObj.get_az(
            self.availability_zone_id)
        if bool(clusters_in_az) != bool(az):
            Logger().warn(
                'Affinity group validation between availability_zone '
                'and na_cluster failed. proceeding...')
        elif not az:
            self.isFirstAzCluster = True
        return 0

    def generateRollbackSQL(self):
        rollbackSqlFileName = (self.argCluster.clusterPath() + '/'
                               + 'rollback_'
                               + self.argCluster.ip + '-'
                               + self.argCluster.timestamp + '-'
                               + dbObj.db_name
                               + '.sql')
        # Rollback SQL queries
        self.storeRollbackSql("/* SQL queries for rollback */")
        self.storeRollbackSql("set autocommit=0;")
        self.storeRollbackSql("select @@autocommit;")
        self.storeRollbackSql("start transaction;")
        self.storeRollbackSql("DELETE FROM na_cluster WHERE "
                              "na_cluster_id='%s';" % self.na_cluster_id)
        self.storeRollbackSql("DELETE FROM na_cluster_capacity WHERE "
                              "na_cluster_id='%s';" % self.na_cluster_id)
        self.storeRollbackSql("DELETE FROM na_cluster_node WHERE "
                              "na_cluster_id='%s';" % self.na_cluster_id)
        self.storeRollbackSql("DELETE FROM na_network_accomodation WHERE "
                              "%s;" % self.nodeIdConcatForQuery())
        self.storeRollbackSql(
            "DELETE FROM na_cluster_vlan_management "
            "WHERE na_cluster_id='%s';" % (
                self.na_cluster_id))

        if self.isFirstVolumeTypeCluster:
            self.storeRollbackSql("DELETE FROM volume_type WHERE "
                                  "volume_type_id='%s';" % self.volume_type_id)

        if self.isFirstAzCluster:
            self.storeRollbackSql("DELETE FROM availability_zone WHERE "
                                  "availability_zone_id='%s';" % self.availability_zone_id)

        # Commit
        self.storeRollbackSql("commit;")

        with open(rollbackSqlFileName, 'w') as outRollbackSqlFile:
            outRollbackSqlFile.write(self.rollbackSqlStatement)
            os.chmod(outRollbackSqlFile.name, 0o664)

        # notification stdout
        print(rollbackSqlFileName)
        Logger().info("Rollback SQL file %s has been created." %
                      rollbackSqlFileName)

    def storeSql(self, statement=''):
        self.sqlStatement += statement + '\n'

    def storeRollbackSql(self, statement=''):
        self.rollbackSqlStatement += statement + '\n'

    def generateInsertQuery(self, table, params, scopes=None):
        valueLengthList = []
        for param in params:
            if len(param) == 1:
                param.append(self.__dict__[param[0]])
            if len(param) > 1 and type(param[1]) == list:
                valueLengthList.append(len(param[1]))
            else:
                param[1] = [param[1]]
                valueLengthList.append(1)
        maxValueLength = max(valueLengthList)

        sql_statement = 'INSERT INTO `%s`\n(' % table
        sql_statement += ', '.join([x[0] for x in params])
        sql_statement += ') SELECT ' if scopes else ') VALUES\n('
        statementList = []
        for iteration in range(0, maxValueLength):
            valueList = []
            for param in params:
                value = 'null' if param[1] == [] else param[1][iteration]
                value = str(value) if type(value) in (
                    int, SqlNonString) else "'" + str(value) + "'"
                valueList.append(value)
            statementList.append(', '.join(valueList))
        sql_statement += '),\n('.join(statementList)
        if scopes:
            sql_statement += ' FROM DUAL WHERE NOT EXISTS(SELECT * FROM %s WHERE ' % table
            for scope in scopes:
                value = params[scope][1][0]
                value = str(value) if type(value) in (
                    int, SqlNonString) else "'" + str(value) + "'"
                delimiter = ' AND ' if scopes.index(
                    scope) != len(scopes) - 1 else ''
                sql_statement += params[scope][0] + \
                    "=" + str(value) + delimiter
        sql_statement += ');'
        return sql_statement

    def generateDataCollectorSettings(self):
        dcQuery = []
        dcRollbackQuery = []
        if not self.volume_type_name == 'piops_iscsi_na':
            return
        from ctrl import config
        from ctrl.database import DataBase
        DbNaStorageEventsObj = DataBase(config["DATABASE"]['host'],
                                        3306,
                                        config["DATABASE"]['user'],
                                        config["DATABASE"]['password'],
                                        config["DATABASE"]['db_name2'])

        # check database
        function_ids = DbNaStorageEventsObj.get_datacollector_function_details()
        mgr_id = DbNaStorageEventsObj.get_datacollector_manager_id()[0][
            'datacollector_manager_id']
        already_registered_cluster_ids = DbNaStorageEventsObj.get_registered_cluster_ids_in_data_collector()

        if self.na_cluster_id in already_registered_cluster_ids:
            Logger().err('cluster is already registered in datacollector_worker_details')

        # generate queries
        dcQuery.append('SET AUTOCOMMIT=0;')
        dcQuery.append('START TRANSACTION;')
        dcQuery.append('')
        dcQuery.append('INSERT INTO datacollector_worker_details(datacollector_worker_id, datacollector_manager_id, na_cluster_id, maintenance_flg, debug_flg, delete_flg, record_created_at, record_updated_at, record_deleted_at) VALUES')

        worker_id = str(uuid.uuid4())
        dcQuery.append('("' + worker_id + '", "' + mgr_id + '", "' +
                       self.na_cluster_id + '", 0, 0, 0, now(), NULL, NULL);')
        dcQuery.append('')
        dcQuery.append('INSERT INTO datacollector_worker_function(datacollector_worker_id, datacollector_function_id, delete_flg, record_created_at, record_updated_at, record_deleted_at) VALUES')

        dcQuery.append(',\n'.join(['("' + worker_id + '", "' + function_id[
                       'datacollector_function_id'] + '", 0, now(), NULL, NULL)' for function_id in function_ids]) + ';\n')
        dcQuery.append('COMMIT;')
        fileName = self.argCluster.clusterPath() + '/' + self.argCluster.ip + '-' + \
            self.argCluster.timestamp + '-' + DbNaStorageEventsObj.db_name + '.sql'

        try:
            with open(fileName, 'w') as outSqlFile:
                outSqlFile.write('\n'.join(dcQuery))
                os.chmod(outSqlFile.name, 0o664)

            # notification stdout
            print(fileName)
            Logger().info("SQL file %s created." % fileName)
        except:
            Logger().err('SQL file creation failed: %s' % fileName)

        # generate rollback queries
        dcRollbackQuery.append('SET AUTOCOMMIT=0;')
        dcRollbackQuery.append('START TRANSACTION;')
        dcRollbackQuery.append('')
        dcRollbackQuery.append('DELETE FROM datacollector_worker_details WHERE datacollector_worker_id="%s" AND na_cluster_id="%s";' % (
            worker_id, self.na_cluster_id))
        dcRollbackQuery.append(
            'DELETE FROM datacollector_worker_function WHERE datacollector_worker_id="%s";' % worker_id)
        dcRollbackQuery.append('COMMIT;')
        rollbackFileName = self.argCluster.clusterPath() + '/rollback_' + self.argCluster.ip + \
            '-' + self.argCluster.timestamp + '-' + DbNaStorageEventsObj.db_name + '.sql'

        try:
            with open(rollbackFileName, 'w') as outSqlFile:
                outSqlFile.write('\n'.join(dcRollbackQuery))
                os.chmod(outSqlFile.name, 0o664)

            # notification stdout
            print(rollbackFileName)
            Logger().info("Rollback SQL file %s created." % rollbackFileName)
        except:
            Logger().err('Rollback SQL file creation failed: %s' % rollbackFileName)

    def generateSQLStatements(self):
        fileName = self.argCluster.clusterPath() + '/' + self.argCluster.ip + '-' + \
            self.argCluster.timestamp + '-' + dbObj.db_name + '.sql'

        self.storeSql("SET autocommit=0;")
        self.storeSql("SELECT @@autocommit;")
        self.storeSql("START TRANSACTION;")
        # na_cluster
        self.storeSql("/*na_cluster*/")

        params = [['na_cluster_id'],
                  ['na_cluster_name'],
                  ['na_cluster_mgmt_ip'],
                  ['na_username'],
                  ['na_password'],
                  ['na_SVM_protocol'],
                  ['availability_zone_id'],
                  ['datacenter_id'],
                  ['floor_name'],
                  ['room_name'],
                  ['rack_info'],
                  ['volume_type_id'],
                  ['na_aggregate_name'],
                  ['na_data_plane_if_name'],
                  ['na_data_plane_mtu'],
                  ['na_storage_plane_if_name'],
                  ['na_storage_plane_mtu'],
                  ['status']]
        self.storeSql(self.generateInsertQuery('na_cluster', params))
        self.storeSql("")

        # na_cluster_capacity
        self.storeSql("/*na_cluster_capacity*/")
        params = [
            ['na_cluster_id'],
            ['na_availability_zone_id'],
            ['max_svms'],
            ['svm_threshold'],
            ['total_threshold'],
            ['total_reserved'],
            ['total_throughput_threshold'],
            ['total_throughput_reserved'],
            ['total_throughput'],
            ['total_iops_threshold'],
            ['total_iops_reserved'],
            ['total_iops']]
        self.storeSql(self.generateInsertQuery('na_cluster_capacity', params))
        self.storeSql("")

        # na_cluster_node
        self.storeSql("/*na_cluster_node*/")
        num_nodes = len(self.argCluster.nodes)

        params = [['na_cluster_node_id', [x.id for x in self.argCluster.nodes]],
                  ['na_cluster_id', [self.na_cluster_id] * num_nodes],
                  ['na_node_name', [x.name for x in self.argCluster.nodes]],
                  ['na_data_plane_if_mac_addres', [
                      x.macAddr for y in self.argCluster.nodes for x in y.ports if x.plane == 'data']],
                  ['na_storage_plane_if_mac_address', [x.macAddr for y in self.argCluster.nodes for x in y.ports if x.plane == 'storage']]]
        self.storeSql(self.generateInsertQuery('na_cluster_node', params))

        self.storeSql("")

        # na_network_accomodation
        self.storeSql("/*na_network_accomodation*/")

        params = [
                 ['ese_physical_device_id', [
                     x.eseDevice for y in self.argCluster.nodes for x in y.ports]],
                 ['ese_physical_port_id', [
                     x.esePhysicalPort for y in self.argCluster.nodes for x in y.ports]],
                 ['physical_port_id', [
                     x.physicalPort for y in self.argCluster.nodes for x in y.ports]],
                 ['na_cluster_node_id', [
                     y.id for y in self.argCluster.nodes for x in y.ports]],
                 ['na_ifgroup_name', [
                     x.name for y in self.argCluster.nodes for x in y.ports]],
                 ['na_ifgroup_mac_address', [x.macAddr for y in self.argCluster.nodes for x in y.ports]]]
        self.storeSql(self.generateInsertQuery(
            'na_network_accomodation', params))
        self.storeSql()

        # na_cluster_vlan_management
        self.storeSql("/*na_cluster_vlan_management*/")

        qty_of_vlan = (self.vlan_id_end - self.vlan_id_start + 1)
        params = [
            ['na_cluster_id', [self.na_cluster_id] * qty_of_vlan],
            ['na_vlan_id', range(self.vlan_id_start,
                                 self.vlan_id_end + 1)],
            ['na_vlan_status', ['vacant'] * qty_of_vlan],
            ['created_at', [SqlNonString('now()')] * qty_of_vlan]]
        self.storeSql(self.generateInsertQuery(
            'na_cluster_vlan_management', params))
        self.storeSql()

        # volume_type
        if self.isFirstVolumeTypeCluster:
            params = [['volume_type_id'],
                      ['volume_type_name'],
                      ['created_at', SqlNonString('now()')],
                      ['updated_at', SqlNonString('NULL')],
                      ['deleted_at', SqlNonString('NULL')]]
            scopes = [0, 1]
            self.storeSql(self.generateInsertQuery(
                'volume_type', params, scopes))
            self.storeSql()

        # az
        if self.isFirstAzCluster:
            params = [['region_id', dbObj.get_region_id()],
                      ['availability_zone_id'],
                      ['availability_zone_name', 'zone1-group' + self.argCluster.affGrp.lower()],
                      ['status', 'available']]
            scopes = [0, 1]
            self.storeSql(self.generateInsertQuery(
                'availability_zone', params, scopes))
            self.storeSql()

        # Confirm all tables modified
        self.storeSql("/* Confirm changes done in all tables "
                      "by using following commands */")
        self.storeSql("SELECT * FROM na_cluster WHERE na_cluster_id='%s';" %
                      self.na_cluster_id)
        self.storeSql("SELECT * FROM na_cluster_capacity WHERE "
                      "na_cluster_id='%s';" % self.na_cluster_id)
        self.storeSql("SELECT * FROM na_cluster_node WHERE "
                      "na_cluster_id='%s';" % self.na_cluster_id)
        self.storeSql("SELECT * FROM na_network_accomodation WHERE "
                      "%s;" % self.nodeIdConcatForQuery())
        self.storeSql(
            "SELECT * FROM na_cluster_vlan_management "
            "WHERE na_cluster_id='%s';" % (
                self.na_cluster_id))
        self.storeSql("SELECT * FROM volume_type;")
        self.storeSql("SELECT * FROM availability_zone;")
        self.storeSql("COMMIT;\n")

        try:
            with open(fileName, 'w') as outSqlFile:
                outSqlFile.write(self.sqlStatement)
                os.chmod(outSqlFile.name, 0o664)

            # notification stdout
            print(fileName)
            Logger().info("SQL file %s created." % fileName)
        except:
            Logger().err('SQL file creation failed: %s' % fileName)

    def insert(self, db_name):
        insertQuery = [x.replace('\n', '') for x in self.sqlStatement]
        selectQuery = [x.replace('\n', '') for x in self.sqlStatement if re.search(
            r'^SELECT .* (FROM|from)', x)]
        # inserting query
        try:
            dbObj._update_data_without_commit('\n'.join(insertQuery))
            dbObj._commit()
        except:
            Logger().err("Can't connect to DB server.", exit=False)
            Logger().err(traceback.format_exc())

        # select confirmation query
        try:
            for statement in selectQuery:
                dbObj._select_data(statement)
        except:
            Logger().err("Can't connect to DB server.", exit=False)
            Logger().err(traceback.format_exc())

    def __init__(self, argCluster=None):
        if argCluster:
            self.isFirstVolumeTypeCluster = False
            self.isFirstAzCluster = False
            self.sqlStatement = ''
            self.rollbackSqlStatement = ''
            self.argCluster = argCluster
            # Variables need to be editted
            self.vlan_id_start = 11
            self.vlan_id_end = 1000

            # cluster variables
            self.na_cluster_id = argCluster.id
            self.na_cluster_name = argCluster.name
            self.cluster_type = argCluster.cluster_type
            self.na_cluster_mgmt_ip = argCluster.ip
            self.na_SVM_protocol = argCluster.protocol  # "iscsi" or "nfs"
            if argCluster.affGrp == 'A':
                self.availability_zone_id = "0c409be7-2559-437a-9331-62e05e44096f"
            elif argCluster.affGrp == 'B':
                self.availability_zone_id = "6d356a65-5ff4-4ce2-aa3c-cad5c4b4a6bc"
            elif argCluster.affGrp == 'C':
                self.availability_zone_id = "f9c6074a-b65f-4911-b939-0c3b14339884"
            elif argCluster.affGrp == 'D':
                self.availability_zone_id = "93801f35-ee0a-4896-9724-679dc5653b52"
            else:
                Logger().err("Affinity Group should be A,B,C or D.")

            self.na_availability_zone_id = self.availability_zone_id
            self.na_username = "api"
            self.na_password = "ef3f73ECLBo!"
            self.floor_name = "floor_name"
            self.room_name = "room_name"
            self.rack_info = "rack_info"

            # Static variables
            # 1st DC in region
            self.datacenter_id = "c18678ff-5123-4cd6-b6e2-62c07af09c17"
            self.na_aggregate_name = "aggr0"
            self.na_storage_plane_if_name = "a0a"
            self.na_storage_plane_mtu = 9000
            self.status = "active"
            self.max_svms = 250
            self.svm_threshold = 75
            self.total_reserved = 0
            self.total_throughput_reserved = 0
            self.total_iops_reserved = 0

            if self.cluster_type is '':
                # piops_iscsi_na
                self.volume_type_id = "6328d234-7939-4d61-9216-736de66d15f9"
                self.volume_type_name = 'piops_iscsi_na'
                self.na_data_plane_if_name = "a0b"
                self.na_data_plane_mtu = 1500
                self.total_throughput_threshold = 0
                self.total_throughput = 0
                self.total_iops = 64000
                self.total_iops_threshold = 100
                # Current total_threshold conditions per regions
                # reference
                # https://confluence.ntt.eu/display/CloudCity/Storage+SDP+-+DB+Tables+Pre-populated#StorageSDP-DBTablesPre-populated-na_cluster_capacitytable
                # UK1/US1/AU1/HK1 : 50%
                # otherwise: 75%
                # as of 2018.1
                if argCluster.name[:3] in ('sy1', 'fd2'):
                    self.total_threshold = 50
                else:
                    self.total_threshold = 75
            elif self.cluster_type == 'nfs':
                # pre_nfs_na
                self.volume_type_id = "bf33db2a-d13e-11e5-8949-005056ab5d30"
                self.volume_type_name = 'pre_nfs_na'
                self.na_data_plane_if_name = "null"
                self.na_data_plane_mtu = 0
                self.total_throughput_threshold = 300
                self.total_throughput = 1600
                self.total_threshold = 50
                self.total_iops = 0
                self.total_iops_threshold = 0
            elif self.cluster_type == 'cluster':
                # standard_nfs_na
                self.volume_type_id = "704db6e5-8a93-41a5-850d-405913600341"
                self.volume_type_name = 'standard_nfs_na'
                self.na_data_plane_if_name = "a0b"
                self.na_data_plane_mtu = 1500
                self.total_throughput_threshold = 300
                self.total_throughput = 2200
                self.total_threshold = 75
                self.total_iops = 0
                self.total_iops_threshold = 0

            else:
                Logger().err(
                    "cluster_type(e.g. kw1ax-[here]00010002n)is unknown.")

            self.volume_type_validation()
            self.az_validation()

    def update(self, fileName=None):
        if not dbObj.check_master():
            Logger().err('DB server %s is not master. Specify master DB in env file. Exitting...')
        if fileName:
            reDbName = re.compile(r'(rollback_)*[0-9]{,3}.[0-9]{,3}.[0-9]{,3}.[0-9]{,3}-[0-9]{8}-[0-9]{6}-(.*).sql')
            flgRollback = reDbName.search(fileName).group(1) is not None
            db_name = reDbName.search(fileName).group(2)
            dbObj.db_name = db_name
        self.readSqlFile(fileName)
        error_flg = False
        if db_name == 'storage_sdp':
            error_flg = self.check_duplication_in_na_cluster()
        if not error_flg or flgRollback:
            dbObj.start_transaction()
            if not hasattr(dbObj, 'connection'):
                Logger().err('DB connection failed.')
            self.insert(db_name)
            Logger().info('DB Insertion for cluster %s completed.' % self.na_cluster_name)
        else:
            Logger().err('Cluster %s is already installed in Storage SDP.' %
                         self.na_cluster_name)

    def select(self, fileName=None):
        if fileName:
            reDbName = re.compile(r'[0-9]{8}-[0-9]{6}-(.*).sql')
            db_name = reDbName.search(fileName).group(1)
            dbObj.db_name = db_name
        self.readSqlFile(fileName)
        selectQuery = [x.replace('\n', '') for x in self.sqlStatement if re.search(
            r'^SELECT .* (FROM|from)', x)]
        # select confirmation query
        try:
            for statement in selectQuery:
                dbObj._select_data_with_stdout(statement)
        except:
            Logger().err("Can't connect to DB server.", exit=False)
            Logger().err(traceback.format_exc())

    def readSqlFile(self, fileName=None):
        if not fileName:
            Logger().err('Specify file name.')
        with open(fileName, 'r') as file:
            self.sqlStatement = file.read().split('\n')
        reClusterName = re.compile(
            r'([a-zA-Z]{2}[0-9])([a-z])x-(nfs|cluster)*[0-9]{4}([0-9]{4})*n')
        findClusterName = reClusterName.search(fileName)
        if findClusterName:
            self.na_cluster_name = str(findClusterName.group(0))
            self.cluster_type = findClusterName.group(3)

    def generate(self):
        self.generateSQLStatements()
        self.generateRollbackSQL()
        if dbObj.check_db_existence('na_storage_events'):
            self.generateDataCollectorSettings()

