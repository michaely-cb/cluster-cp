"""
Node Survey module for linkmon
Not used in prometheus linkmon. When running in standalone mode, linkmon only
runs on the node where the user started the run, and that node connects to all
cluster nodes over SSH, returning node NIC info as lines of CSV.
"""
import asyncio
import logging
import os
import re
import sqlite3
import sys
import time

import asyncssh
from nodeinfo import NodeInfo


def int_or_zero(s):
    try:
        return int(s)
    except ValueError:
        return 0


class NodeInfoWithDB(NodeInfo):
    def __init__(self, host="", info_csv=None, db_row=None):
        if db_row:  # only used for non-prom runs when running under dbtool
            # self.key = db_row["mac"]
            self.host = db_row["Host"]
            self.ifname = db_row["IfName"]
            self.mac = db_row["mac"]
            self.carrier = int(db_row["carrier"])
            self.carrier_changes = int(db_row["Flaps"])
            self.operstate = db_row["OperState"]
            self.speed = int(db_row["Speed"])
            # self.iftype = int(fields[6])
            self.supports_1g = bool(db_row["supports_1g"])
            self.supports_100g = bool(db_row["supports_100g"])
            self.ethtool_error = int(db_row["DriverOK"])
            # track this? self.has_mlx = int(fields[10]) > 0

            self.rx_crc_errors_phy = int_or_zero(db_row["RxCRCErr"])
            self.rx_bytes_phy = int_or_zero(db_row["RxKBytes"])
            self.rx_pcs_symbol_err_phy = int_or_zero(db_row["RxPCSErr"])
            self.rx_corrected_bits_phy = int_or_zero(db_row["RxCorrectedBit"])
            self.rx_err_lane_0_phy = int_or_zero(db_row["RxErrL0"])
            self.rx_err_lane_1_phy = int_or_zero(db_row["RxErrL1"])
            self.rx_err_lane_2_phy = int_or_zero(db_row["RxErrL2"])
            self.rx_err_lane_3_phy = int_or_zero(db_row["RxErrL3"])
            self.boottime = db_row["boottime"]
            self.supports_10g = bool(db_row["supports_10g"])
            self.tx_cqe_err = int_or_zero(db_row["TxCQEErr"])
            self.rx_wqe_err = int_or_zero(db_row["RxWQEErr"])
            self.local_ack_timeout_err = int_or_zero(db_row["LocalAckTimeoutErr"])
            self.implied_nak_seq_err = int_or_zero(db_row["ImpliedNAKSeqErr"])
            self.out_of_seq = int_or_zero(db_row["OutOfSeq"])
        else:
            super().__init__(host, info_csv)


class NodeSurvey:
    fmt_str = "%-20s %-15s %-18s %7s %-8s %-14s %-9s %5s %14s %9s %9s %15s %8s %8s %8s %8s %8s %8s %8s %9s %8s %-8s"
    headings = (
        "Host",
        "IfName",
        "MAC",
        "Speed",
        "Carrier",
        "OperState",
        "DriverOK",
        "Flaps",
        "RxKBytes",
        "RxCRCErr",
        "RxPCSErr",
        "RxCorrectedBit",
        "RxErrL0",
        "RxErrL1",
        "RxErrL2",
        "RxErrL3",
        "LAckTOut",
        "ImpNakSeq",
        "OutOfSeq",
        "Health",
    )

    # methods below this line are not used in prometheus mode
    def __init__(self, node_list, include_1g, ssh_key):
        self.include_1g = include_1g
        self.ssh_key = ssh_key
        self.stats = dict()
        self.key = 0
        self.results = None

        if node_list:
            self.nodes = list(node_list)
            self.key = 0

            t1 = time.time()
            self.collect()
            t2 = time.time()
            self.populate()
            t3 = time.time()

            self.collect_time = t2 - t1
            self.process_time = t3 - t2
            return

    @classmethod
    def sqlite_create_table_cmd(cls):
        node_create_query = """
CREATE TABLE IF NOT EXISTS node (
id INTEGER PRIMARY KEY
"""
        for hkey in cls.headings:
            hkey = re.sub(r" ", "_", hkey)
            node_create_query += f",{hkey} TEXT NOT NULL"

        node_create_query += ",boottime STRING, supports_1g INTEGER,supports_100g INTEGER"

        node_create_query += ");"
        return node_create_query

    @staticmethod
    async def run_client(host, cmd, user, password, sshkey) -> asyncssh.SSHCompletedProcess:
        if sshkey:
            keyarg = [sshkey]
        else:
            keyarg = None
        async with asyncssh.connect(
            host, username=user, password=password, client_keys=keyarg, known_hosts=None
        ) as conn:
            return await conn.run(cmd, check=True)

    async def run_all(self) -> None:
        node_user = os.getenv('NODE_USER', 'root')  # default
        node_pass = os.getenv('NODE_PASSWORD', 'root')
        tasks = (
            NodeSurvey.run_client(n, NodeInfo.node_cmdstring, node_user, node_pass, self.ssh_key)
            for n in self.nodes
        )
        self.results = await asyncio.gather(*tasks, return_exceptions=True)

    def collect(self):
        asyncio.get_event_loop().run_until_complete(self.run_all())

    def populate(self):
        for i, result in enumerate(self.results):
            if isinstance(result, Exception):
                logging.error('SSH failure on %s: %s' % (self.nodes[i], str(result)))
            elif result.exit_status != 0:
                logging.error('Command failed on %s: %s' % (self.nodes[i], result.exit_status))
            else:
                for line in result.stdout.split("\n"):
                    if line == "":
                        continue
                    ni = NodeInfoWithDB(self.nodes[i], line)
                    if ni.supports_100g or ni.ethtool_error or (self.include_1g and ni.supports_1g):
                        self.stats[ni.mac] = ni

    @staticmethod
    def print_header(dest=sys.stdout):
        print(NodeSurvey.fmt_str % NodeSurvey.headings, file=dest)

    @classmethod
    def print_two_runs(cls, survey1, survey2, dest=sys.stdout):
        init_set = set(survey1.stats.keys())
        final_set = set(survey2.stats.keys())

        up_down_set = init_set - final_set
        down_up_set = final_set - init_set
        for k in sorted((init_set | final_set)):  # everything
            if k in init_set:
                i = common = survey1.stats[k]
            if k in final_set:
                f = common = survey2.stats[k]

            if k in up_down_set:
                carrier = str(i.carrier) + " -> ?"
                operstate = i.operstate + " -> ?"
                ethtool_error = str(int(not i.ethtool_error)) + " -> ?"
                flaps = rxbytes = crc_err = pcs_err = corr = '?'
                err0 = err1 = err2 = err3 = '?'
                cqe = wqe = '?'
                l_ack_timeout = imp_nak_seq = out_of_seq = '?'
                health = "BAD"
            elif k in down_up_set:
                carrier = "? -> " + str(f.carrier)
                operstate = "? -> " + f.operstate
                ethtool_error = "? -> " + str(int(not f.ethtool_error))
                flaps = rxbytes = crc_err = pcs_err = corr = '?'
                err0 = err1 = err2 = err3 = '?'
                cqe = wqe = '?'
                l_ack_timeout = imp_nak_seq = out_of_seq = '?'
                health = "BAD"
            else:
                carrier = str(i.carrier) + " -> " + str(f.carrier)
                operstate = i.operstate + " -> " + f.operstate
                ethtool_error = str(int(not i.ethtool_error)) + " -> " + str(int(not f.ethtool_error))
                flaps = f.carrier_changes - i.carrier_changes
                rxbytes = int((f.rx_bytes_phy - i.rx_bytes_phy) / 1024)
                crc_err = f.rx_crc_errors_phy - i.rx_crc_errors_phy
                pcs_err = f.rx_pcs_symbol_err_phy - i.rx_pcs_symbol_err_phy
                corr = f.rx_corrected_bits_phy - i.rx_corrected_bits_phy
                err0 = f.rx_err_lane_0_phy - i.rx_err_lane_0_phy
                err1 = f.rx_err_lane_0_phy - i.rx_err_lane_0_phy
                err2 = f.rx_err_lane_0_phy - i.rx_err_lane_0_phy
                err3 = f.rx_err_lane_0_phy - i.rx_err_lane_0_phy
                cqe = f.tx_cqe_err - i.tx_cqe_err
                wqe = f.rx_wqe_err - i.rx_wqe_err
                l_ack_timeout = f.local_ack_timeout_err - i.local_ack_timeout_err
                imp_nak_seq = f.imp_nak_seq - i.imp_nak_seq
                out_of_seq = f.out_of_seq - i.out_of_seq
                if f.carrier_changes != i.carrier_changes:
                    health = "FLAP"
                elif not f.ethtool_error and f.operstate == "up":
                    if (
                        f.rx_crc_errors_phy - i.rx_crc_errors_phy != 0
                        or f.rx_pcs_symbol_err_phy - i.rx_pcs_symbol_err_phy != 0
                    ):  # or f.rx_corrected_bits_phy-i.rx_corrected_bits_phy != 0:
                        health = "UNCORRECTED_ERRORS"
                    elif (f.rx_corrected_bits_phy - i.rx_corrected_bits_phy) / (
                        1 + f.rx_bytes_phy - i.rx_bytes_phy
                    ) / 1024 > 1:
                        health = "MANY_CORRECTED_ERRORS"
                    elif f.rx_corrected_bits_phy - i.rx_corrected_bits_phy > 0:
                        health = "SOME_CORRECTED"
                    else:
                        health = "GOOD"
                else:
                    health = "BAD"

            out_tuple = (
                common.host,
                common.ifname,
                common.mac,
                common.speed,
                carrier,
                operstate,
                ethtool_error,
                flaps,
                rxbytes,
                crc_err,
                pcs_err,
                corr,
                err0,
                err1,
                err2,
                err3,
                cqe,
                wqe,
                l_ack_timeout,
                imp_nak_seq,
                out_of_seq,
                health,
            )
            print(NodeSurvey.fmt_str % out_tuple, file=dest)

    def insert_run(self, dbfile):
        if not self.stats:
            return

        con = sqlite3.connect(dbfile)
        cur = con.cursor()

        for s in self.stats:
            dbcmd = "INSERT INTO node VALUES(?,   ?, ?, ?, ?,   ?, ?, ?, ?,   ?, ?, ?, ?,   ?, ?, ?, ?,   ?,?,?,?, ?,?,?,?, ?)"
            data = (
                None,
                self.stats[s].host,
                self.stats[s].ifname,
                self.stats[s].mac,
                self.stats[s].speed,
                self.stats[s].carrier,
                self.stats[s].operstate,
                self.stats[s].ethtool_error,
                self.stats[s].carrier_changes,
                self.stats[s].rx_bytes_phy,
                self.stats[s].rx_crc_errors_phy,
                self.stats[s].rx_pcs_symbol_err_phy,
                self.stats[s].rx_corrected_bits_phy,
                self.stats[s].rx_err_lane_0_phy,
                self.stats[s].rx_err_lane_1_phy,
                self.stats[s].rx_err_lane_2_phy,
                self.stats[s].rx_err_lane_3_phy,
                self.stats[s].tx_cqe_err,
                self.stats[s].rx_wqe_err,
                self.stats[s].local_ack_timeout_err,
                self.stats[s].implied_nak_seq_err,
                self.stats[s].out_of_seq,
                self.stats[s].boottime,
                self.stats[s].supports_1g,
                self.stats[s].supports_100g,
                "",
            )
            cur.execute(dbcmd, data)

            self.stats[s].key = cur.lastrowid

        surveycmd = "INSERT INTO survey(survey_type) VALUES('node')"
        cur.execute(surveycmd)
        survey_id = cur.lastrowid

        for s in self.stats:
            dscmd = f"INSERT INTO device_survey(survey, devicequery) VALUES(?, ?)"
            data = (survey_id, self.stats[s].key)
            cur.execute(dscmd, data)

        con.commit()
        con.close()
        self.key = survey_id

    def from_db(self, con, node_id):
        cur = con.cursor()
        # get last runs
        qstring = """
SELECT *
FROM device_survey
    INNER JOIN node ON device_survey.devicequery = node.id
WHERE device_survey.survey = ?
"""
        res = cur.execute(qstring, (node_id,))
        nodes = res.fetchall()
        for node in nodes:
            ni = NodeInfoWithDB(host=None, info_csv=None, db_row=node)
            self.stats[ni.mac] = ni


def main():
    if len(sys.argv) < 2:
        sys.exit()
    if sys.argv[1] != "test":
        sys.exit()

    s1list = []
    s2list = []
    if len(sys.argv) > 2 and sys.argv[2] and sys.argv[2] != "-":
        s1list = [sys.argv[2]]
    if len(sys.argv) > 3 and sys.argv[3] and sys.argv[3] != "-":
        s2list = [sys.argv[3]]
    survey1 = NodeSurvey(s1list, True, False)
    survey2 = NodeSurvey(s2list, True, False)

    NodeSurvey.print_header()
    NodeSurvey.print_two_runs(survey1, survey2)


if __name__ == "__main__":
    main()
